use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, LazyLock, OnceLock};

use arrow_array::{Array, BinaryArray, BooleanArray};
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion::scalar::ScalarValue;
use geo::{Coord, coord};
use geo_traits::{
    CoordTrait, Dimensions, GeometryCollectionTrait, GeometryTrait, LineStringTrait, LineTrait,
    MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait, RectTrait,
    TriangleTrait,
};
use geoarrow_array::array::{WkbArray, from_arrow_array};
use geoarrow_array::builder::GeometryCollectionBuilder;
use geoarrow_array::cast::{from_wkb, to_wkb};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::error::GeoArrowError;
use geoarrow_schema::{CoordType, GeoArrowType, GeometryType, Metadata};
use wkb::reader::Wkb;
use wkb::writer::{WriteOptions, geometry_wkb_size, write_geometry};
use wkt::types::{
    Coord as WktCoord, Dimension as WktDimension, GeometryCollection, LineString, MultiLineString,
};

use crate::data_types::any_geometry_type;
use crate::error::GeoDataFusionResult;

/// A single geometry argument, optionally followed by the `directed` boolean.
static SIGNATURE: LazyLock<Signature> = LazyLock::new(|| {
    let geometry_types = any_geometry_type();
    let mut variants = Vec::with_capacity(geometry_types.len() * 2);
    for geometry_type in geometry_types {
        variants.push(TypeSignature::Exact(vec![geometry_type.clone()]));
        variants.push(TypeSignature::Exact(vec![geometry_type, DataType::Boolean]));
    }
    Signature::one_of(variants, Volatility::Immutable)
});

/// Sews together the component lines of a (multi)linestring.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct LineMerge {
    coord_type: CoordType,
}

impl LineMerge {
    /// Creates a new `ST_LineMerge` UDF instance with the specified output [`CoordType`].
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for LineMerge {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for LineMerge {
    fn name(&self) -> &str {
        "st_linemerge"
    }

    fn signature(&self) -> &Signature {
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field_impl(args, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(line_merge_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a (set of) LineString(s) formed by sewing together the constituent line work of a MultiLineString. Lines are joined at endpoints where exactly two lines meet; lines are not merged across intersections of three or more lines. When `directed` is true, lines are only merged when their directions agree. Inputs without linework yield an empty GeometryCollection. This function strips the M dimension.",
                "ST_LineMerge(geometry, directed)",
            )
            .with_argument("geom", "geometry")
            .with_argument("directed", "boolean")
            .build()
        }))
    }
}

fn return_field_impl(
    args: ReturnFieldArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<FieldRef> {
    let metadata = Arc::new(Metadata::try_from(args.arg_fields[0].as_ref()).unwrap_or_default());
    let output_type = GeometryType::new(metadata).with_coord_type(coord_type);
    Ok(Arc::new(output_type.to_field("", true)))
}

/// Parse the optional `directed` argument.
///
/// Absent or null is treated as `false`.
fn parse_directed(args: &ScalarFunctionArgs) -> GeoDataFusionResult<bool> {
    match args.args.get(1) {
        None => Ok(false),
        Some(arg) => match arg.cast_to(&DataType::Boolean, None)? {
            ColumnarValue::Scalar(ScalarValue::Boolean(directed)) => Ok(directed.unwrap_or(false)),
            // A cast to `Boolean` only ever yields a `Boolean` scalar.
            ColumnarValue::Scalar(_) => unreachable!("cast to Boolean yields a Boolean scalar"),
            // Python's DataFusion FFI expands scalar arguments to the input array's length.
            ColumnarValue::Array(array) => {
                let array = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "cast to Boolean yields a Boolean array".to_string(),
                        )
                    })?;
                let directed = array
                    .iter()
                    .map(|value| value.unwrap_or(false))
                    .try_fold(None, |directed, value| match directed {
                        None => Ok(Some(value)),
                        Some(directed) if directed == value => Ok(Some(directed)),
                        _ => Err(DataFusionError::NotImplemented(
                            "A varying `directed` argument to ST_LineMerge is not yet implemented"
                                .to_string(),
                        )),
                    })
                    .map(Option::unwrap_or_default)?;
                Ok(directed)
            }
        },
    }
}

fn line_merge_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let directed = parse_directed(&args)?;
    let arrays = ColumnarValue::values_to_arrays(&args.args[0..1])?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let metadata = geo_array.data_type().metadata().clone();
    let wkb_array = to_wkb::<i32>(&*geo_array)?;
    // Normalize nested GeometryCollections through WKB, whose children retain their own dimensions.
    let wkb_array = to_wkb::<i32>(&wkb_array)?;
    let mut merged = Vec::with_capacity(wkb_array.len());

    // WKT coordinates retain Z while the endpoint graph below uses XY for
    // connectivity. GEOS also discards M from the merged result.
    for maybe_wkb in wkb_array.inner() {
        merged.push(match maybe_wkb {
            None => None,
            Some(wkb) => {
                let geometry = Wkb::try_new(wkb)?;
                if is_empty(&geometry) {
                    Some(wkb.to_vec())
                } else {
                    let lines = merged_line_strings(&geometry, directed);
                    let geometry = lines_to_geometry(lines);
                    let mut bytes = Vec::with_capacity(geometry_wkb_size(&geometry));
                    write_geometry(&mut bytes, &geometry, &WriteOptions::default())?;
                    Some(bytes)
                }
            }
        });
    }

    let result_wkb = WkbArray::new(merged.into_iter().collect::<BinaryArray>(), metadata);
    let to_type = GeoArrowType::from_arrow_field(args.return_field.as_ref())?;
    let result = match to_type {
        GeoArrowType::Geometry(typ) => {
            let mut builder = crate::udf::native::io::geometry::GeometryArrayBuilder::new(typ);
            for geometry in result_wkb.iter() {
                builder.push_geometry(geometry.transpose()?.as_ref())?;
            }
            Arc::new(builder.finish()) as Arc<dyn GeoArrowArray>
        }
        GeoArrowType::GeometryCollection(typ) => {
            // A typed collection cannot hold a bare merged LineString. Wrap non-empty results in
            // one collection, as required by the native GeoArrow collection representation.
            let typ = infer_collection_type(&result_wkb, typ)?;
            let mut builder =
                crate::udf::native::io::geometry::GeometryCollectionArrayBuilder::new(typ.clone());
            for geometry in result_wkb.iter() {
                let geometry = geometry.transpose()?;
                if geometry.is_some() {
                    let collection = GeometryCollectionBuilder::from_nullable_geometries(
                        &[geometry.as_ref()],
                        typ.clone(),
                    )?
                    .finish();
                    let collection = collection.value(0)?;
                    builder.push_geometry_collection(Some(&collection))?;
                } else {
                    builder.push_geometry_collection(None::<&GeometryCollection>)?;
                }
            }
            Arc::new(builder.finish()) as Arc<dyn GeoArrowArray>
        }
        typ => from_wkb(&result_wkb, typ)?,
    };
    Ok(ColumnarValue::Array(result.to_array_ref()))
}

fn infer_collection_type(
    wkb: &WkbArray,
    typ: geoarrow_schema::GeometryCollectionType,
) -> GeoDataFusionResult<geoarrow_schema::GeometryCollectionType> {
    let dimension = wkb
        .iter()
        .map(|geometry| geometry.transpose())
        .collect::<Result<Vec<_>, GeoArrowError>>()?
        .into_iter()
        .flatten()
        .find_map(|geometry| match geometry.dim() {
            Dimensions::Xyz | Dimensions::Xyzm => Some(geoarrow_schema::Dimension::XYZ),
            Dimensions::Xym => Some(geoarrow_schema::Dimension::XYM),
            Dimensions::Xy | Dimensions::Unknown(_) => None,
        })
        .unwrap_or(geoarrow_schema::Dimension::XY);
    Ok(typ.with_dimension(dimension))
}

fn is_empty(geometry: &impl GeometryTrait<T = f64>) -> bool {
    match geometry.as_type() {
        geo_traits::GeometryType::Point(point) => point.coord().is_none(),
        geo_traits::GeometryType::LineString(line) => line.num_coords() == 0,
        geo_traits::GeometryType::Polygon(polygon) => polygon.exterior().is_none(),
        geo_traits::GeometryType::MultiPoint(points) => {
            points.points().all(|p| p.coord().is_none())
        }
        geo_traits::GeometryType::MultiLineString(lines) => {
            lines.line_strings().all(|l| l.num_coords() == 0)
        }
        geo_traits::GeometryType::MultiPolygon(polygons) => {
            polygons.polygons().all(|p| p.exterior().is_none())
        }
        geo_traits::GeometryType::GeometryCollection(collection) => {
            collection.geometries().all(|g| is_empty(&g))
        }
        geo_traits::GeometryType::Rect(_)
        | geo_traits::GeometryType::Triangle(_)
        | geo_traits::GeometryType::Line(_) => false,
    }
}

fn merged_line_strings(geometry: &impl GeometryTrait<T = f64>, directed: bool) -> Vec<LineString> {
    let mut edges = Vec::new();
    collect_lines(geometry, &mut edges);
    let lines = merge_lines(edges, directed);
    // GEOS promotes mixed XYZ/XY linework to XYZ, filling absent Z ordinates with NaN. Use one
    // dimension across the result so MultiLineString headers and coordinates stay consistent.
    let has_z = lines.iter().flatten().any(|coord| coord.z.is_some());
    let dimension = if has_z {
        WktDimension::XYZ
    } else {
        WktDimension::XY
    };
    lines
        .into_iter()
        .map(|mut coords| {
            if has_z {
                for coord in &mut coords {
                    coord.z.get_or_insert(f64::NAN);
                }
            }
            LineString::new(coords, dimension)
        })
        .collect()
}

fn lines_to_geometry(lines: Vec<LineString>) -> wkt::Wkt {
    match lines.len() {
        0 => wkt::Wkt::GeometryCollection(GeometryCollection::empty(WktDimension::XY)),
        1 => wkt::Wkt::LineString(lines.into_iter().next().unwrap()),
        _ => {
            let dimension = lines[0].dimension();
            wkt::Wkt::MultiLineString(MultiLineString::new(lines, dimension))
        }
    }
}

// Use a total floating-point order for graph keys, normalizing signed zero to match XY equality.
// This keeps equality, hashing, and traversal ordering consistent even for NaN coordinates.
#[derive(Clone, Copy, Debug)]
struct Node([u64; 2]);

impl From<WktCoord> for Node {
    fn from(coord: WktCoord) -> Self {
        Self([
            canonical_coordinate(coord.x).to_bits(),
            canonical_coordinate(coord.y).to_bits(),
        ])
    }
}

fn canonical_coordinate(value: f64) -> f64 {
    if value.is_nan() || value == 0.0 {
        // `total_cmp` treats all NaNs and both signed zeros as equal.
        if value.is_nan() { f64::NAN } else { 0.0 }
    } else {
        value
    }
}

impl Node {
    fn x(&self) -> f64 {
        f64::from_bits(self.0[0])
    }

    fn y(&self) -> f64 {
        f64::from_bits(self.0[1])
    }
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Node {}

impl Hash for Node {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl Ord for Node {
    fn cmp(&self, other: &Self) -> Ordering {
        self.x()
            .total_cmp(&other.x())
            .then_with(|| self.y().total_cmp(&other.y()))
    }
}

impl PartialOrd for Node {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug)]
struct HalfEdge {
    edge: usize,
    next: WktCoord,
    forward: bool,
}

fn collect_lines(geometry: &impl GeometryTrait<T = f64>, edges: &mut Vec<Vec<WktCoord>>) {
    use geo_traits::GeometryType;

    match geometry.as_type() {
        GeometryType::LineString(line) => collect_line(line.coords(), edges),
        GeometryType::Polygon(polygon) => {
            if let Some(exterior) = polygon.exterior() {
                collect_line(exterior.coords(), edges);
            }
            for interior in polygon.interiors() {
                collect_line(interior.coords(), edges);
            }
        }
        GeometryType::MultiLineString(lines) => {
            for line in lines.line_strings() {
                collect_line(line.coords(), edges);
            }
        }
        GeometryType::MultiPolygon(polygons) => {
            for polygon in polygons.polygons() {
                collect_lines(&polygon, edges);
            }
        }
        GeometryType::GeometryCollection(collection) => {
            for child in collection.geometries() {
                collect_lines(&child, edges);
            }
        }
        GeometryType::Rect(rect) => {
            let min = rect.min();
            let max = rect.max();
            collect_line(
                [
                    (min.x(), min.y()),
                    (max.x(), min.y()),
                    (max.x(), max.y()),
                    (min.x(), max.y()),
                    (min.x(), min.y()),
                ]
                .into_iter()
                .map(|(x, y)| fill_coord(x, y, geometry.dim())),
                edges,
            );
        }
        GeometryType::Triangle(triangle) => {
            let [a, b, c] = triangle.coords();
            let dimension = geometry.dim();
            let a = fill_coord(a.x(), a.y(), dimension);
            let b = fill_coord(b.x(), b.y(), dimension);
            let c = fill_coord(c.x(), c.y(), dimension);
            collect_line([a, b, c, a].into_iter(), edges);
        }
        GeometryType::Line(line) => collect_line([line.start(), line.end()].into_iter(), edges),
        GeometryType::Point(_) | GeometryType::MultiPoint(_) => {}
    }
}

fn collect_line<'a>(
    coords: impl Iterator<Item = impl CoordTrait<T = f64> + 'a>,
    edges: &mut Vec<Vec<WktCoord>>,
) {
    let mut coords = coords.map(coord_to_wkt).collect::<Vec<_>>();
    coords.dedup_by(|a, b| a.x == b.x && a.y == b.y);
    if coords.len() > 1 {
        edges.push(coords);
    }
}

fn coord_to_wkt(coord: impl CoordTrait<T = f64>) -> WktCoord {
    WktCoord {
        x: coord.x(),
        y: coord.y(),
        z: coord
            .nth(2)
            .filter(|_| matches!(coord.dim(), Dimensions::Xyz | Dimensions::Xyzm)),
        m: None,
    }
}

fn fill_coord(x: f64, y: f64, dimension: Dimensions) -> WktCoord {
    WktCoord {
        x,
        y,
        z: matches!(dimension, Dimensions::Xyz | Dimensions::Xyzm).then_some(f64::NAN),
        m: None,
    }
}

fn merge_lines(edges: Vec<Vec<WktCoord>>, directed: bool) -> Vec<Vec<WktCoord>> {
    // Match GEOS's traversal so component and vertex order remain compatible:
    // visit nodes in XY order and their incident edges in angular order.
    let mut nodes = HashMap::<Node, Vec<HalfEdge>>::new();
    let mut node_order = Vec::new();
    for (edge, coords) in edges.iter().enumerate() {
        let start = coords[0];
        let end = coords[coords.len() - 1];
        if !nodes.contains_key(&Node::from(start)) {
            node_order.push(Node::from(start));
        }
        if !nodes.contains_key(&Node::from(end)) {
            node_order.push(Node::from(end));
        }
        nodes.entry(Node::from(start)).or_default().push(HalfEdge {
            edge,
            next: coords[1],
            forward: true,
        });
        nodes.entry(Node::from(end)).or_default().push(HalfEdge {
            edge,
            next: coords[coords.len() - 2],
            forward: false,
        });
    }
    for (node, half_edges) in &mut nodes {
        half_edges.sort_by(|a, b| {
            let a = coord! { x: a.next.x - node.x(), y: a.next.y - node.y() };
            let b = coord! { x: b.next.x - node.x(), y: b.next.y - node.y() };
            compare_direction(a, b)
        });
    }

    let mut used = vec![false; edges.len()];
    let mut result = Vec::new();
    node_order.sort();
    for node in &node_order {
        let half_edges = &nodes[node];
        let start =
            half_edges.len() != 2 || directed && half_edges[0].forward == half_edges[1].forward;
        if start {
            for half_edge in half_edges {
                if (!directed || half_edge.forward) && !used[half_edge.edge] {
                    result.push(build_line(half_edge, &nodes, &edges, &mut used, directed));
                }
            }
        }
    }
    // GEOS starts isolated rings from their least node and preserves angular edge ordering.
    for node in &node_order {
        for half_edge in &nodes[node] {
            if (!directed || half_edge.forward) && !used[half_edge.edge] {
                result.push(build_line(half_edge, &nodes, &edges, &mut used, directed));
            }
        }
    }
    result
}

fn build_line(
    start: &HalfEdge,
    nodes: &HashMap<Node, Vec<HalfEdge>>,
    edges: &[Vec<WktCoord>],
    used: &mut [bool],
    directed: bool,
) -> Vec<WktCoord> {
    let mut coords = Vec::new();
    let mut current = start;
    let mut forward_edges = 0;
    let mut reverse_edges = 0;
    loop {
        used[current.edge] = true;
        let source = &edges[current.edge];
        if current.forward {
            forward_edges += 1;
        } else {
            reverse_edges += 1;
        }
        if current.forward {
            coords.extend(source.iter().copied().skip(usize::from(!coords.is_empty())));
        } else {
            coords.extend(
                source
                    .iter()
                    .rev()
                    .copied()
                    .skip(usize::from(!coords.is_empty())),
            );
        }
        let end = if current.forward {
            source.last().unwrap()
        } else {
            &source[0]
        };
        let Some(half_edges) = nodes.get(&Node::from(*end)) else {
            break;
        };
        if half_edges.len() != 2 {
            break;
        }
        let next = if half_edges[0].edge == current.edge {
            &half_edges[1]
        } else {
            &half_edges[0]
        };
        if directed && !next.forward || next.edge == start.edge {
            break;
        }
        current = next;
    }
    if reverse_edges > forward_edges {
        coords.reverse();
    }
    coords
}

fn compare_direction(a: Coord, b: Coord) -> Ordering {
    quadrant(a)
        .cmp(&quadrant(b))
        .then_with(|| ((b.x * a.y) - (b.y * a.x)).total_cmp(&0.0))
}

fn quadrant(coord: Coord) -> u8 {
    match (coord.x.total_cmp(&0.0), coord.y.total_cmp(&0.0)) {
        (Ordering::Greater | Ordering::Equal, Ordering::Greater | Ordering::Equal) => 0,
        (Ordering::Less, Ordering::Greater | Ordering::Equal) => 1,
        (Ordering::Less | Ordering::Equal, Ordering::Less) => 2,
        (Ordering::Greater, Ordering::Less) => 3,
    }
}

#[cfg(test)]
mod test {
    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::udf::native::io::{AsText, GeomFromText};

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udf(LineMerge::default().into());
        ctx.register_udf(GeomFromText::default().into());
        ctx.register_udf(AsText.into());
        ctx
    }

    #[test]
    fn test_node_key_contract() {
        let positive_zero = Node::from(WktCoord {
            x: 0.0,
            y: 0.0,
            z: None,
            m: None,
        });
        let negative_zero = Node::from(WktCoord {
            x: -0.0,
            y: -0.0,
            z: None,
            m: None,
        });
        assert_eq!(positive_zero, negative_zero);
        let mut first = std::collections::hash_map::DefaultHasher::new();
        let mut second = std::collections::hash_map::DefaultHasher::new();
        positive_zero.hash(&mut first);
        negative_zero.hash(&mut second);
        assert_eq!(first.finish(), second.finish());

        let nodes = [
            Node::from(WktCoord {
                x: f64::NAN,
                y: 0.0,
                z: None,
                m: None,
            }),
            Node::from(WktCoord {
                x: -f64::NAN,
                y: 0.0,
                z: None,
                m: None,
            }),
        ];
        let mut nodes = nodes;
        nodes.sort();
        assert_eq!(nodes[0], nodes[1]);
    }

    #[tokio::test]
    async fn test_st_linemerge() {
        let ctx = ctx();

        // Explicitly noted examples come from the PostGIS documentation (CC-BY-SA-3.0).
        let cases = vec![
            (
                "MULTILINESTRING((10 160, 60 120), (120 140, 60 120), (120 140, 180 120))",
                "LINESTRING(10 160,60 120,120 140,180 120)",
                "PostGIS doc example: lines meeting two-at-a-time sew into one LineString",
            ),
            (
                "MULTILINESTRING((10 160, 60 120), (120 140, 60 120), (120 140, 180 120), (100 180, 120 140))",
                "MULTILINESTRING((10 160,60 120,120 140),(100 180,120 140),(120 140,180 120))",
                "degree-3 node: merge does not cross a junction of three lines",
            ),
            (
                "MULTILINESTRING((-29 -27,-30 -29.7,-36 -31,-45 -33),(-45.2 -33.2,-46 -32))",
                "MULTILINESTRING((-45.2 -33.2,-46 -32),(-29 -27,-30 -29.7,-36 -31,-45 -33))",
                "disjoint components are returned unchanged (as a MultiLineString)",
            ),
            (
                "LINESTRING(0 0, 1 1)",
                "LINESTRING(0 0,1 1)",
                "a lone LineString round-trips unchanged",
            ),
            (
                "POINT(0 0)",
                "GEOMETRYCOLLECTION EMPTY",
                "input with no line work yields an empty GeometryCollection",
            ),
            (
                "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
                "LINESTRING(0 0,1 0,1 1,0 1,0 0)",
                "PostGIS behavior: polygon boundary rings are returned as closed LineStrings",
            ),
            (
                "LINESTRING EMPTY",
                "LINESTRING EMPTY",
                "PostGIS gotcha: empty input is returned as-is rather than collapsed to an empty GeometryCollection",
            ),
            (
                "GEOMETRYCOLLECTION(GEOMETRYCOLLECTION Z(MULTILINESTRING Z((0 0 1,1 1 2),(1 1 3,2 2 4))))",
                "LINESTRING Z(0 0 1,1 1 2,2 2 4)",
                "mixed-dimensional nested collections preserve child Z coordinates",
            ),
            (
                "MULTILINESTRING ZM((0 0 1 10,1 1 2 20),(1 1 3 30,2 2 4 40))",
                "LINESTRING Z(0 0 1,1 1 2,2 2 4)",
                "GEOS preserves Z and discards M when joining segments",
            ),
        ];

        for (input, expected, description) in cases {
            let sql = if input.starts_with("GEOMETRYCOLLECTION") {
                format!("SELECT ST_AsText(ST_LineMerge('{}'))", input)
            } else {
                format!(
                    "SELECT ST_AsText(ST_LineMerge(ST_GeomFromText('{}')))",
                    input
                )
            };
            let df = ctx
                .sql(&sql)
                .await
                .unwrap_or_else(|_| panic!("Failed to execute SQL for {}", description));

            let batch = df.collect().await.unwrap().into_iter().next().unwrap();
            let val = batch.column(0).as_string::<i32>().value(0);

            assert_eq!(val, expected, "Failed on {}: {}", description, input);
        }
    }

    #[tokio::test]
    async fn test_st_linemerge_directed() {
        let ctx = ctx();

        // Same input, contrasting the directed flag: with TRUE the disagreeing segments stay
        // split; with FALSE (the default) they all merge into a single LineString.
        let input = "MULTILINESTRING((60 30, 10 70), (120 50, 60 30), (120 50, 180 30))";
        let cases = vec![
            (
                input,
                "TRUE",
                "MULTILINESTRING((120 50,60 30,10 70),(120 50,180 30))",
                "directed: segments whose directions disagree are not merged",
            ),
            (
                input,
                "FALSE",
                "LINESTRING(180 30,120 50,60 30,10 70)",
                "undirected: same input merges fully when direction is ignored",
            ),
            (
                "MULTILINESTRING((0 0,1 1),(1 1,1 0),(1 0,0 0))",
                "TRUE",
                "LINESTRING(0 0,1 1,1 0,0 0)",
                "directed: a closed ring is emitted once even when its incoming edge sorts first",
            ),
        ];

        for (input, directed, expected, description) in cases {
            let sql = format!(
                "SELECT ST_AsText(ST_LineMerge(ST_GeomFromText('{}'), {}))",
                input, directed
            );
            let df = ctx
                .sql(&sql)
                .await
                .unwrap_or_else(|_| panic!("Failed to execute SQL for {description}"));

            let batch = df.collect().await.unwrap().into_iter().next().unwrap();
            let val = batch.column(0).as_string::<i32>().value(0);

            assert_eq!(
                val, expected,
                "Failed on {description}: directed={directed}",
            );
        }
    }

    #[tokio::test]
    async fn test_st_linemerge_null() {
        let ctx = ctx();

        // Null geometries propagate to null, including alongside non-null rows in the same array.
        let df = ctx
            .sql(
                "WITH t(wkt) AS (VALUES ('LINESTRING(0 0, 1 1)'), (CAST(NULL AS TEXT))) \
                 SELECT ST_AsText(ST_LineMerge(ST_GeomFromText(wkt))) FROM t ORDER BY wkt NULLS LAST",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0).as_string::<i32>();

        assert_eq!(col.value(0), "LINESTRING(0 0,1 1)");
        assert!(
            col.is_null(1),
            "null input should propagate to a null output"
        );
    }

    #[tokio::test]
    async fn test_st_linemerge_rejects_non_boolean_directed() {
        let ctx = ctx();
        let result = ctx
            .sql("SELECT ST_LineMerge(ST_GeomFromText('LINESTRING(0 0,1 1)'), 'invalid')")
            .await;
        assert!(result.is_err());
    }

    #[test]
    fn test_native_collection_return() -> GeoDataFusionResult<()> {
        use datafusion::config::ConfigOptions;
        use geoarrow_array::cast::AsGeoArrowArray;
        use geoarrow_schema::{Dimension, GeometryCollectionType};

        let input = WkbArray::new(
            [Some(encode_wkt(
                "GEOMETRYCOLLECTION(POINT(8 8),LINESTRING Z(0 0 7,1 1 8))",
            ))]
            .into_iter()
            .collect::<BinaryArray>(),
            Default::default(),
        );
        let input = input.to_array_ref();
        let input_field = Arc::new(arrow_schema::Field::new("geometry", DataType::Binary, true));
        let typ = GeoArrowType::GeometryCollection(GeometryCollectionType::new(
            Dimension::XY,
            Default::default(),
        ));
        let output = line_merge_impl(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(input)],
            arg_fields: vec![input_field],
            number_rows: 1,
            return_field: Arc::new(typ.to_field("geometry", true)),
            config_options: Arc::new(ConfigOptions::default()),
        })?;
        let ColumnarValue::Array(output) = output else {
            unreachable!()
        };
        let output_type = GeoArrowType::GeometryCollection(GeometryCollectionType::new(
            Dimension::XYZ,
            Default::default(),
        ));
        let output = from_arrow_array(output.as_ref(), &output_type.to_field("geometry", true))?;
        let geometry = output.as_geometry_collection().value(0)?;
        assert_eq!(geometry.num_geometries(), 1);
        let line = geometry.geometries().next().unwrap();
        assert_eq!(line.dim(), Dimensions::Xyz);
        Ok(())
    }

    fn encode_wkt(wkt: &str) -> Vec<u8> {
        let geometry: wkt::Wkt = wkt.parse().unwrap();
        let mut bytes = Vec::new();
        write_geometry(&mut bytes, &geometry, &WriteOptions::default()).unwrap();
        bytes
    }

    #[test]
    fn test_mixed_dimension_lines_are_promoted() {
        let geometry: wkt::Wkt =
            "GEOMETRYCOLLECTION Z(LINESTRING Z(0 0 7,1 1 8),LINESTRING(2 2,3 3))"
                .parse()
                .unwrap();
        let geometry = merged_line_strings(&geometry, false);
        let geometry = lines_to_geometry(geometry);
        assert_eq!(geometry.dimension(), WktDimension::XYZ);
        let wkt::Wkt::MultiLineString(lines) = geometry else {
            panic!("expected MultiLineString")
        };
        let lines = lines.line_strings().iter().collect::<Vec<_>>();
        assert!(lines[0].coords().iter().all(|coord| coord.z.is_some()));
        assert!(
            lines[1]
                .coords()
                .iter()
                .all(|coord| coord.z.unwrap().is_nan())
        );
    }
}

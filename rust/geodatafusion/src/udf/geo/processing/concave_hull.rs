use std::collections::HashMap;
use std::sync::{Arc, LazyLock, OnceLock};

use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{Array, BooleanArray, PrimitiveArray};
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use geo::algorithm::concave_hull::ConcaveHullOptions;
use geo::{
    ConcaveHull as _, ConvexHull as _, Coord, LineString, MultiPoint, Point, Polygon, Validation,
};
use geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, LineStringTrait, LineTrait,
    MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait, RectTrait,
    TriangleTrait,
};
use geoarrow_array::array::{GeometryArray, from_arrow_array};
use geoarrow_array::builder::GeometryBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::{CoordType, GeometryType, Metadata};
use wkt::Wkt;
use wkt::types::{
    Coord as WktCoord, Dimension as WktDimension, GeometryCollection as WktGeometryCollection,
    LineString as WktLineString, MultiLineString as WktMultiLineString,
    MultiPoint as WktMultiPoint, MultiPolygon as WktMultiPolygon, Point as WktPoint,
    Polygon as WktPolygon,
};

use crate::data_types::any_geometry_type;
use crate::error::GeoDataFusionResult;
use crate::udf::native::accessors::is_geometry_topologically_empty;

static SIGNATURE: LazyLock<Signature> =
    LazyLock::new(|| Signature::user_defined(Volatility::Immutable));

/// `ST_ConcaveHull` scalar UDF implementation.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct ConcaveHull {
    coord_type: CoordType,
}

impl ConcaveHull {
    /// Creates a new `ST_ConcaveHull` UDF with the requested output coordinate layout.
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for ConcaveHull {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for ConcaveHull {
    fn name(&self) -> &str {
        "st_concavehull"
    }

    fn signature(&self) -> &Signature {
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "Return field is computed from input metadata in return_field_from_args.".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let metadata = metadata_for_field(args.arg_fields[0].as_ref())?;
        let output_type = GeometryType::new(metadata).with_coord_type(self.coord_type);
        Ok(Arc::new(output_type.to_field("", true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(concave_hull_impl(args, self.coord_type)?)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if !(2..=3).contains(&arg_types.len()) {
            return Err(DataFusionError::Plan(format!(
                "ST_ConcaveHull expects 2 or 3 arguments, got {}",
                arg_types.len()
            )));
        }

        let geometry_type = &arg_types[0];
        let supported_geometry = geometry_type == &DataType::Null
            || any_geometry_type()
                .iter()
                .any(|candidate| candidate == geometry_type);
        if !supported_geometry {
            return Err(DataFusionError::Plan(format!(
                "ST_ConcaveHull does not support input type {geometry_type}"
            )));
        }

        if !arg_types[1].is_numeric() && arg_types[1] != DataType::Null {
            return Err(DataFusionError::Plan(format!(
                "ST_ConcaveHull expects a numeric pctconvex value, got {}",
                arg_types[1]
            )));
        }
        if arg_types.len() == 3
            && arg_types[2] != DataType::Boolean
            && arg_types[2] != DataType::Null
        {
            return Err(DataFusionError::Plan(format!(
                "ST_ConcaveHull expects a boolean allow_holes value, got {}",
                arg_types[2]
            )));
        }

        let mut coerced = Vec::with_capacity(arg_types.len());
        coerced.push(geometry_type.clone());
        coerced.push(DataType::Float64);
        if arg_types.len() == 3 {
            coerced.push(DataType::Boolean);
        }
        Ok(coerced)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Computes a possibly concave geometry containing all input vertices. \
                 A pctconvex value of 1 returns the convex hull; lower values produce increasingly \
                 concave hulls. The result is a Point for one or more identical points, a \
                 LineString for collinear points, and a Polygon otherwise. Inputs that are \
                 topologically empty preserve their geometry type. Existing polygon holes are \
                 retained only when allow_holes is true; new point-set holes are not generated. \
                 The M coordinate is not preserved. Vertex selection can differ from PostGIS \
                 because this implementation uses the geo crate's concave-hull algorithm rather \
                 than GEOS.",
                "ST_ConcaveHull(geometry, pctconvex[, allow_holes])",
            )
            .with_argument("geometry", "geometry")
            .with_argument("pctconvex", "float in [0, 1]")
            .with_argument("allow_holes", "boolean")
            .build()
        }))
    }
}

fn metadata_for_field(field: &arrow_schema::Field) -> Result<Arc<Metadata>> {
    if field.data_type() == &DataType::Null {
        Ok(Arc::new(Metadata::default()))
    } else {
        Ok(Arc::new(Metadata::try_from(field)?))
    }
}

fn validate_pctconvex(pctconvex: f64, error: impl FnOnce(String) -> DataFusionError) -> Result<()> {
    if !pctconvex.is_finite() || !(0.0..=1.0).contains(&pctconvex) {
        return Err(error(format!(
            "ST_ConcaveHull pctconvex must be finite and in [0, 1], got {pctconvex}"
        )));
    }
    Ok(())
}

fn concave_hull_impl(
    args: ScalarFunctionArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    if arrays[0].data_type() == &DataType::Null {
        return Ok(null_geometry_array(args.number_rows, coord_type)?
            .into_array_ref()
            .into());
    }

    let pctconvex = arrays[1].as_primitive::<Float64Type>();
    let allow_holes = arrays.get(2).map(|array| array.as_boolean());
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let output_type =
        GeometryType::new(geo_array.data_type().metadata().clone()).with_coord_type(coord_type);
    let geo_array_ref = geo_array.as_ref();
    let output = downcast_geoarrow_array!(
        geo_array_ref,
        concave_hull_array,
        &output_type,
        args.number_rows,
        pctconvex,
        allow_holes
    )?;
    Ok(output.into_array_ref().into())
}

fn concave_hull_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    output_type: &GeometryType,
    number_rows: usize,
    pctconvex: &PrimitiveArray<Float64Type>,
    allow_holes: Option<&BooleanArray>,
) -> GeoDataFusionResult<GeometryArray> {
    let mut builder = GeometryBuilder::new(output_type.clone());

    for index in 0..number_rows {
        let Some(pctconvex) = primitive_at(pctconvex, index) else {
            builder.push_null();
            continue;
        };
        let allow_holes = match allow_holes {
            Some(values) => match bool_at(values, index) {
                Some(value) => value,
                None => {
                    builder.push_null();
                    continue;
                }
            },
            None => false,
        };

        let geometry_index = broadcast_index(index, array.len());
        if array.is_null(geometry_index) {
            builder.push_null();
            continue;
        }
        validate_pctconvex(pctconvex, DataFusionError::Execution)?;
        let geometry = array.value(geometry_index)?;
        let output = concave_hull_geometry(&geometry, pctconvex, allow_holes)?;
        builder.push_geometry(Some(&output))?;
    }

    Ok(builder.finish())
}

fn primitive_at<T: arrow_array::ArrowPrimitiveType>(
    array: &PrimitiveArray<T>,
    index: usize,
) -> Option<T::Native> {
    let index = broadcast_index(index, array.len());
    (!array.is_null(index)).then(|| array.value(index))
}

fn bool_at(array: &BooleanArray, index: usize) -> Option<bool> {
    let index = broadcast_index(index, array.len());
    (!array.is_null(index)).then(|| array.value(index))
}

fn broadcast_index(index: usize, array_len: usize) -> usize {
    if array_len == 1 { 0 } else { index }
}

fn null_geometry_array(
    number_rows: usize,
    coord_type: CoordType,
) -> GeoDataFusionResult<GeometryArray> {
    let output_type = GeometryType::new(Default::default()).with_coord_type(coord_type);
    let mut builder = GeometryBuilder::new(output_type);
    for _ in 0..number_rows {
        builder.push_null();
    }
    Ok(builder.finish())
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct CoordKey {
    x: u64,
    y: u64,
}

impl CoordKey {
    fn new(x: f64, y: f64) -> Self {
        Self {
            x: canonical_float_bits(x),
            y: canonical_float_bits(y),
        }
    }
}

fn canonical_float_bits(value: f64) -> u64 {
    if value == 0.0 { 0 } else { value.to_bits() }
}

#[derive(Clone, Copy, Debug)]
struct HullCoord {
    x: f64,
    y: f64,
    z: Option<f64>,
}

impl HullCoord {
    fn key(self) -> CoordKey {
        CoordKey::new(self.x, self.y)
    }

    fn geo(self) -> Coord<f64> {
        Coord {
            x: self.x,
            y: self.y,
        }
    }

    fn wkt(self, dimension: WktDimension) -> WktCoord<f64> {
        WktCoord {
            x: self.x,
            y: self.y,
            z: (dimension == WktDimension::XYZ).then_some(self.z).flatten(),
            m: None,
        }
    }
}

fn concave_hull_geometry(
    geometry: &impl GeometryTrait<T = f64>,
    pctconvex: f64,
    allow_holes: bool,
) -> GeoDataFusionResult<Wkt<f64>> {
    if is_geometry_topologically_empty(geometry) {
        return empty_geometry(geometry);
    }

    let has_z = geometry_has_z(geometry.dim());
    let dimension = output_wkt_dimension(geometry.dim())?;
    let mut coords = Vec::new();
    collect_coords(geometry, has_z, &mut coords)?;
    let unique = unique_finite_coords(coords)?;
    if unique.is_empty() {
        return empty_geometry(geometry);
    }

    let mut by_key = HashMap::with_capacity(unique.len());
    for coord in &unique {
        by_key.entry(coord.key()).or_insert(*coord);
    }

    if unique.len() == 1 {
        return Ok(Wkt::Point(WktPoint::new(
            Some(unique[0].wkt(dimension)),
            dimension,
        )));
    }

    let endpoints = collinear_endpoints(&unique);
    if let Some((start, end)) = endpoints {
        return Ok(Wkt::LineString(WktLineString::new(
            vec![start.wkt(dimension), end.wkt(dimension)],
            dimension,
        )));
    }

    let points = MultiPoint::new(
        unique
            .iter()
            .map(|coord| Point::from(coord.geo()))
            .collect(),
    );
    let hull = if pctconvex == 1.0 {
        points.convex_hull()
    } else {
        points.concave_hull_with_options(ConcaveHullOptions {
            concavity: concavity_for_pctconvex(pctconvex),
            length_threshold: 0.0,
        })
    };
    let polygon = hull_to_wkt(&hull, dimension, has_z, &by_key, geometry, allow_holes)?;
    Ok(Wkt::Polygon(polygon))
}

fn empty_geometry(geometry: &impl GeometryTrait<T = f64>) -> GeoDataFusionResult<Wkt<f64>> {
    let dimension = output_wkt_dimension(geometry.dim())?;
    Ok(match geometry.as_type() {
        geo_traits::GeometryType::Point(_) => Wkt::Point(WktPoint::empty(dimension)),
        geo_traits::GeometryType::LineString(_) => Wkt::LineString(WktLineString::empty(dimension)),
        geo_traits::GeometryType::Polygon(_) => Wkt::Polygon(WktPolygon::empty(dimension)),
        geo_traits::GeometryType::MultiPoint(_) => Wkt::MultiPoint(WktMultiPoint::empty(dimension)),
        geo_traits::GeometryType::MultiLineString(_) => {
            Wkt::MultiLineString(WktMultiLineString::empty(dimension))
        }
        geo_traits::GeometryType::MultiPolygon(_) => {
            Wkt::MultiPolygon(WktMultiPolygon::empty(dimension))
        }
        geo_traits::GeometryType::GeometryCollection(_) => {
            Wkt::GeometryCollection(WktGeometryCollection::empty(dimension))
        }
        geo_traits::GeometryType::Rect(_)
        | geo_traits::GeometryType::Triangle(_)
        | geo_traits::GeometryType::Line(_) => {
            return Err(DataFusionError::Execution(
                "ST_ConcaveHull received an empty Rect, Triangle, or Line".to_string(),
            )
            .into());
        }
    })
}

fn collect_coords(
    geometry: &impl GeometryTrait<T = f64>,
    has_z: bool,
    output: &mut Vec<HullCoord>,
) -> GeoDataFusionResult<()> {
    use geo_traits::GeometryType::*;

    match geometry.as_type() {
        Point(point) => {
            if let Some(coord) = point.coord() {
                output.push(hull_coord(coord, has_z));
            }
        }
        LineString(line) => append_line_coords(line, has_z, output)?,
        Polygon(polygon) => {
            if let Some(ring) = polygon.exterior() {
                append_line_coords(&ring, has_z, output)?;
            }
            for ring in polygon.interiors() {
                append_line_coords(&ring, has_z, output)?;
            }
        }
        MultiPoint(points) => {
            for point in points.points() {
                if let Some(coord) = point.coord() {
                    output.push(hull_coord(coord, has_z));
                }
            }
        }
        MultiLineString(lines) => {
            for line in lines.line_strings() {
                append_line_coords(&line, has_z, output)?;
            }
        }
        MultiPolygon(polygons) => {
            for polygon in polygons.polygons() {
                if let Some(ring) = polygon.exterior() {
                    append_line_coords(&ring, has_z, output)?;
                }
                for ring in polygon.interiors() {
                    append_line_coords(&ring, has_z, output)?;
                }
            }
        }
        GeometryCollection(collection) => {
            for child in collection.geometries() {
                collect_coords(&child, has_z, output)?;
            }
        }
        Rect(rect) => {
            let min = rect.min();
            let max = rect.max();
            let min_x = min.x();
            let min_y = min.y();
            let max_x = max.x();
            let max_y = max.y();
            let min_z = min.nth(2).filter(|_| has_z);
            let max_z = max.nth(2).filter(|_| has_z);
            output.extend([
                HullCoord {
                    x: min_x,
                    y: min_y,
                    z: min_z,
                },
                HullCoord {
                    x: min_x,
                    y: max_y,
                    z: min_z,
                },
                HullCoord {
                    x: max_x,
                    y: max_y,
                    z: max_z,
                },
                HullCoord {
                    x: max_x,
                    y: min_y,
                    z: max_z,
                },
            ]);
        }
        Triangle(triangle) => {
            output.extend(triangle.coords().map(|coord| hull_coord(coord, has_z)));
        }
        Line(line) => {
            output.extend([
                hull_coord(line.start(), has_z),
                hull_coord(line.end(), has_z),
            ]);
        }
    }
    Ok(())
}

fn append_line_coords(
    line: &impl LineStringTrait<T = f64>,
    has_z: bool,
    output: &mut Vec<HullCoord>,
) -> GeoDataFusionResult<()> {
    for coord in line.coords() {
        output.push(hull_coord(coord, has_z));
    }
    Ok(())
}

fn hull_coord(coord: impl CoordTrait<T = f64>, has_z: bool) -> HullCoord {
    HullCoord {
        x: coord.x(),
        y: coord.y(),
        z: has_z.then(|| coord.nth_or_panic(2)),
    }
}

fn unique_finite_coords(coords: Vec<HullCoord>) -> GeoDataFusionResult<Vec<HullCoord>> {
    let mut seen = HashMap::with_capacity(coords.len());
    let mut unique = Vec::with_capacity(coords.len());
    for coord in coords {
        if !coord.x.is_finite()
            || !coord.y.is_finite()
            || coord.z.is_some_and(|value| !value.is_finite())
        {
            return Err(DataFusionError::Execution(
                "ST_ConcaveHull does not support non-finite coordinates".to_string(),
            )
            .into());
        }
        if seen.insert(coord.key(), ()).is_none() {
            unique.push(coord);
        }
    }
    Ok(unique)
}

fn collinear_endpoints(coords: &[HullCoord]) -> Option<(HullCoord, HullCoord)> {
    if coords.len() == 2 {
        return Some((coords[0], coords[1]));
    }

    let first = coords[0];
    let second = coords[1];
    let origin = robust::Coord { x: 0.0, y: 0.0 };
    let direction = robust::Coord {
        x: second.x - first.x,
        y: second.y - first.y,
    };
    if coords.iter().any(|coord| {
        let translated = robust::Coord {
            x: coord.x - first.x,
            y: coord.y - first.y,
        };
        robust::orient2d(origin, direction, translated) != 0.0
    }) {
        return None;
    }

    let mut farthest = (coords[0], coords[1]);
    let mut max_distance = squared_distance(farthest.0, farthest.1);
    for left in 0..coords.len() {
        for right in left + 1..coords.len() {
            let distance = squared_distance(coords[left], coords[right]);
            if distance > max_distance {
                max_distance = distance;
                farthest = (coords[left], coords[right]);
            }
        }
    }
    Some(farthest)
}

fn squared_distance(left: HullCoord, right: HullCoord) -> f64 {
    let dx = left.x - right.x;
    let dy = left.y - right.y;
    dx * dx + dy * dy
}

fn hull_to_wkt(
    hull: &Polygon<f64>,
    dimension: WktDimension,
    has_z: bool,
    by_key: &HashMap<CoordKey, HullCoord>,
    input: &impl GeometryTrait<T = f64>,
    allow_holes: bool,
) -> GeoDataFusionResult<WktPolygon<f64>> {
    let mut rings = vec![original_ring_to_wkt(hull.exterior(), dimension, by_key)?];
    if allow_holes {
        let mut holes = Vec::new();
        collect_polygon_holes(input, has_z, &mut holes)?;
        let mut valid_holes = Vec::new();
        for hole in holes {
            let Some(candidate_hole) = geo_hole(&hole) else {
                continue;
            };
            let mut candidate_holes = valid_holes.clone();
            candidate_holes.push(candidate_hole.clone());
            let candidate = Polygon::new(hull.exterior().clone(), candidate_holes);
            if candidate.is_valid() {
                valid_holes.push(candidate_hole);
                rings.push(WktLineString::new(
                    hole.into_iter().map(|coord| coord.wkt(dimension)).collect(),
                    dimension,
                ));
            }
        }
    }
    Ok(WktPolygon::new(rings, dimension))
}

fn geo_hole(hole: &[HullCoord]) -> Option<LineString<f64>> {
    if hole.len() < 4 || hole.first()?.key() != hole.last()?.key() {
        return None;
    }
    Some(LineString::new(
        hole.iter().map(|coord| coord.geo()).collect(),
    ))
}

fn original_ring_to_wkt(
    ring: &geo::LineString<f64>,
    dimension: WktDimension,
    by_key: &HashMap<CoordKey, HullCoord>,
) -> GeoDataFusionResult<WktLineString<f64>> {
    let coords = ring
        .coords()
        .map(|coord| {
            by_key
                .get(&CoordKey::new(coord.x, coord.y))
                .copied()
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "ST_ConcaveHull produced a coordinate absent from the input".to_string(),
                    )
                    .into()
                })
                .map(|coord| coord.wkt(dimension))
        })
        .collect::<GeoDataFusionResult<Vec<_>>>()?;
    Ok(WktLineString::new(coords, dimension))
}

fn collect_polygon_holes(
    geometry: &impl GeometryTrait<T = f64>,
    has_z: bool,
    output: &mut Vec<Vec<HullCoord>>,
) -> GeoDataFusionResult<()> {
    match geometry.as_type() {
        geo_traits::GeometryType::Polygon(polygon) => {
            collect_holes_from_polygon(polygon, has_z, output)?;
        }
        geo_traits::GeometryType::MultiPolygon(polygons) => {
            for polygon in polygons.polygons() {
                collect_holes_from_polygon(&polygon, has_z, output)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn collect_holes_from_polygon(
    polygon: &impl PolygonTrait<T = f64>,
    has_z: bool,
    output: &mut Vec<Vec<HullCoord>>,
) -> GeoDataFusionResult<()> {
    for ring in polygon.interiors() {
        let coords = ring
            .coords()
            .map(|coord| hull_coord(coord, has_z))
            .collect();
        output.push(coords);
    }
    Ok(())
}

fn output_wkt_dimension(dimension: geo_traits::Dimensions) -> GeoDataFusionResult<WktDimension> {
    match dimension {
        geo_traits::Dimensions::Xy | geo_traits::Dimensions::Xym => Ok(WktDimension::XY),
        geo_traits::Dimensions::Xyz | geo_traits::Dimensions::Xyzm => Ok(WktDimension::XYZ),
        geo_traits::Dimensions::Unknown(size) => Err(DataFusionError::NotImplemented(format!(
            "ST_ConcaveHull does not support geometries with {size} physical dimensions"
        ))
        .into()),
    }
}

fn geometry_has_z(dimension: geo_traits::Dimensions) -> bool {
    matches!(
        dimension,
        geo_traits::Dimensions::Xyz | geo_traits::Dimensions::Xyzm
    )
}

/// Maps PostGIS's normalized concavity parameter to `geo`'s mapbox-style concavity.
///
/// The endpoints agree: `pctconvex=0` selects `geo`'s most concave useful value,
/// and `pctconvex=1` is handled separately as the convex hull. In between,
/// `geo` searches for a vertex within `edge_length / concavity`, while the
/// PostGIS value scales the allowed edge length from 0 to 1. Setting that
/// search distance to `edge_length * (1 - pctconvex)` gives this monotonic
/// mapping. It is a stable native mapping, not GEOS-equivalent math.
fn concavity_for_pctconvex(pctconvex: f64) -> f64 {
    1.0 / (1.0 - pctconvex)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pctconvex_mapping_is_monotonic_with_expected_endpoints() {
        assert_eq!(concavity_for_pctconvex(0.0), 1.0);
        assert_eq!(concavity_for_pctconvex(0.5), 2.0);
        assert_eq!(concavity_for_pctconvex(0.75), 4.0);
        assert!(concavity_for_pctconvex(1.0).is_infinite());
    }

    #[test]
    fn signed_zero_coordinates_are_identical_points() {
        assert_eq!(CoordKey::new(0.0, -0.0), CoordKey::new(-0.0, 0.0));
    }

    #[test]
    fn non_finite_coordinates_are_rejected() {
        let error = unique_finite_coords(vec![HullCoord {
            x: f64::NAN,
            y: 0.0,
            z: None,
        }])
        .unwrap_err();
        assert!(error.to_string().contains("non-finite"));
    }

    #[test]
    fn invalid_or_unclosed_holes_are_not_emitted() {
        let coords = [
            HullCoord {
                x: 0.0,
                y: 0.0,
                z: None,
            },
            HullCoord {
                x: 1.0,
                y: 0.0,
                z: None,
            },
            HullCoord {
                x: 0.0,
                y: 1.0,
                z: None,
            },
        ];
        assert!(geo_hole(&coords).is_none());

        let hull = Polygon::new(
            LineString::new(vec![
                Coord { x: 0.0, y: 0.0 },
                Coord { x: 2.0, y: 0.0 },
                Coord { x: 2.0, y: 2.0 },
                Coord { x: 0.0, y: 2.0 },
                Coord { x: 0.0, y: 0.0 },
            ]),
            vec![],
        );
        let outside_hole = LineString::new(vec![
            Coord { x: 3.0, y: 3.0 },
            Coord { x: 4.0, y: 3.0 },
            Coord { x: 4.0, y: 4.0 },
            Coord { x: 3.0, y: 3.0 },
        ]);
        assert!(!Polygon::new(hull.exterior().clone(), vec![outside_hole]).is_valid());
    }

    #[test]
    fn hull_to_wkt_skips_holes_that_make_the_polygon_invalid() {
        let hull = Polygon::new(
            LineString::new(vec![
                Coord { x: 0.0, y: 0.0 },
                Coord { x: 2.0, y: 0.0 },
                Coord { x: 2.0, y: 2.0 },
                Coord { x: 0.0, y: 2.0 },
                Coord { x: 0.0, y: 0.0 },
            ]),
            vec![],
        );
        let input = Polygon::new(
            LineString::new(vec![
                Coord { x: -1.0, y: -1.0 },
                Coord { x: 3.0, y: -1.0 },
                Coord { x: 3.0, y: 3.0 },
                Coord { x: -1.0, y: 3.0 },
                Coord { x: -1.0, y: -1.0 },
            ]),
            vec![LineString::new(vec![
                Coord { x: 3.0, y: 3.0 },
                Coord { x: 4.0, y: 3.0 },
                Coord { x: 4.0, y: 4.0 },
                Coord { x: 3.0, y: 3.0 },
            ])],
        );
        let by_key = hull
            .exterior()
            .coords()
            .map(|coord| {
                let coord = HullCoord {
                    x: coord.x,
                    y: coord.y,
                    z: None,
                };
                (coord.key(), coord)
            })
            .collect();

        let output = hull_to_wkt(&hull, WktDimension::XY, false, &by_key, &input, true).unwrap();
        assert_eq!(output.rings().len(), 1);
    }
}

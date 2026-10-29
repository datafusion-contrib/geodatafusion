use std::sync::{Arc, OnceLock};

use arrow_array::builder::StringBuilder;
use arrow_array::{Array, StringArray};
use arrow_schema::{DataType, Field};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, GeometryType as NativeGeometryType,
    LineStringTrait, MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait,
    PolygonTrait,
};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::{CoordType, GeometryType, Metadata};
use wkt::Wkt;
use wkt::types::{
    Coord, Dimension as WktDimension, GeometryCollection, LineString, MultiLineString, MultiPoint,
    MultiPolygon, Point, Polygon,
};

use super::column::{IntColumn, TextColumn};
use super::common::srid_from_metadata;
use super::coordinate::format_ordinate;
use super::xml::{self, XmlNode};
use crate::data_types::any_geometry_type;
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::{
    StrictGeometryBuilder, geometry_array_from_arrow, metadata_for_srid,
};

const KML_NAMESPACE: &str = "http://www.opengis.net/kml/2.2";
const DEFAULT_PRECISION: i32 = 15;
const KML_SRID: i32 = 4326;

#[derive(Debug, Eq, Hash, PartialEq)]
pub struct GeomFromKML {
    coord_type: CoordType,
}

impl GeomFromKML {
    pub(crate) fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }

    fn invoke(&self, args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let text = TextColumn::from_array(Some(&arrays[0]), None)?;
        let metadata = Arc::new(Metadata::try_from(args.return_field.as_ref())?);
        let target = GeometryType::new(metadata).with_coord_type(self.coord_type);
        let mut builder = StrictGeometryBuilder::new(target);

        for row in 0..arrays[0].len() {
            let Some(value) = text.value(row) else {
                builder.push_geometry(None::<&Wkt<f64>>)?;
                continue;
            };
            let geometry = parse_kml(value, self.name())?;
            builder.push_geometry(Some(&geometry))?;
        }

        Ok(ColumnarValue::Array(builder.finish().to_array_ref()))
    }
}

impl Default for GeomFromKML {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static GEOM_FROM_KML_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for GeomFromKML {
    fn name(&self) -> &str {
        "st_geomfromkml"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: OnceLock<Signature> = OnceLock::new();
        SIGNATURE.get_or_init(|| {
            Signature::uniform(
                1,
                vec![DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View],
                Volatility::Immutable,
            )
        })
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<Arc<Field>> {
        let input = &args.arg_fields[0];
        let typ = GeometryType::new(metadata_for_srid(KML_SRID)).with_coord_type(self.coord_type);
        Ok(typ.to_field(input.name(), input.is_nullable()).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.invoke(args).map_err(Into::into)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(GEOM_FROM_KML_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Constructs a geometry from a KML 2.2 geometry fragment using EPSG:4326.",
                "ST_GeomFromKML(kml)",
            )
            .with_argument("kml", "KML geometry fragment")
            .build()
        }))
    }
}

#[derive(Debug, Eq, Hash, PartialEq)]
pub struct AsKML {
    signature: Signature,
}

impl AsKML {
    pub(crate) fn new() -> Self {
        Self {
            signature: as_kml_signature(),
        }
    }
}

impl Default for AsKML {
    fn default() -> Self {
        Self::new()
    }
}

static AS_KML_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for AsKML {
    fn name(&self) -> &str {
        "st_askml"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<Arc<Field>> {
        let input = &args.arg_fields[0];
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(input.name(), DataType::Utf8, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let metadata = Arc::new(Metadata::try_from(args.arg_fields[0].as_ref())?);
        let srid = srid_from_metadata(&metadata, self.name())?;
        if srid == 0 {
            return Err(DataFusionError::Execution(format!(
                "{}: input geometry has unknown (0) SRID",
                self.name()
            )));
        }
        if srid != KML_SRID {
            return Err(DataFusionError::NotImplemented(format!(
                "{}: reprojecting from SRID {srid} to EPSG:4326 is not supported; use EPSG:4326 input",
                self.name()
            )));
        }

        let precision = IntColumn::from_array(arrays.get(1), Some(i64::from(DEFAULT_PRECISION)))?;
        let prefix = TextColumn::from_array(arrays.get(2), Some(""))?;
        let geo_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
        let geo_array = geo_array.as_ref();
        let result = downcast_geoarrow_array!(geo_array, write_kml_array, &precision, &prefix)?;
        Ok(ColumnarValue::Array(Arc::new(result)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(AS_KML_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a geometry as a KML 2.2 element.",
                "ST_AsKML(geometry[, maxdecimaldigits[, nprefix]])",
            )
            .with_argument("geometry", "input geometry with EPSG:4326")
            .with_argument("maxdecimaldigits", "maximum decimal digits")
            .with_argument("nprefix", "namespace prefix")
            .build()
        }))
    }
}

fn as_kml_signature() -> Signature {
    let geometry_types = any_geometry_type();
    let mut signatures = Vec::new();
    for geometry_type in &geometry_types {
        signatures.push(TypeSignature::Exact(vec![geometry_type.clone()]));
        for precision_type in [DataType::Int32, DataType::Int64] {
            signatures.push(TypeSignature::Exact(vec![
                geometry_type.clone(),
                precision_type.clone(),
            ]));
            for prefix_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
                signatures.push(TypeSignature::Exact(vec![
                    geometry_type.clone(),
                    precision_type.clone(),
                    prefix_type.clone(),
                ]));
            }
        }
    }
    Signature::one_of(signatures, Volatility::Immutable)
}

fn write_kml_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    precision: &IntColumn<'_>,
    prefix: &TextColumn<'_>,
) -> GeoDataFusionResult<StringArray> {
    let mut builder = StringBuilder::with_capacity(array.len(), array.len() * 128);
    for (row, item) in array.iter().enumerate() {
        let Some(precision) = precision.value(row) else {
            builder.append_null();
            continue;
        };
        let Some(prefix) = prefix.value(row) else {
            builder.append_null();
            continue;
        };
        let prefix = if prefix.is_empty() {
            String::new()
        } else {
            normalize_kml_prefix(prefix)?
        };

        match item {
            None => builder.append_null(),
            Some(Err(error)) => return Err(error.into()),
            Some(Ok(geometry)) => match write_kml(&geometry, precision, &prefix)? {
                Some(value) => builder.append_value(value),
                None => builder.append_null(),
            },
        }
    }
    Ok(builder.finish())
}

fn normalize_kml_prefix(prefix: &str) -> GeoDataFusionResult<String> {
    let mut characters = prefix.chars();
    let valid = characters
        .next()
        .is_some_and(|character| character == '_' || character.is_ascii_alphabetic())
        && characters.all(|character| {
            character == '_'
                || character == '-'
                || character == '.'
                || character.is_ascii_alphanumeric()
        })
        && !prefix.contains(':');
    if !valid {
        return Err(DataFusionError::Execution(format!(
            "ST_AsKML: invalid XML namespace prefix {prefix:?}"
        ))
        .into());
    }
    Ok(format!("{prefix}:"))
}

fn write_kml(
    geometry: &impl GeometryTrait<T = f64>,
    precision: i64,
    prefix: &str,
) -> Result<Option<String>> {
    if kml_geometry_is_empty(geometry) {
        return Ok(None);
    }
    let precision =
        i32::try_from(precision).unwrap_or(if precision < 0 { i32::MIN } else { i32::MAX });
    let mut output = String::new();
    write_kml_geometry(&mut output, geometry, precision, prefix)?;
    Ok(Some(output))
}

fn write_kml_geometry(
    output: &mut String,
    geometry: &impl GeometryTrait<T = f64>,
    precision: i32,
    prefix: &str,
) -> Result<()> {
    match geometry.as_type() {
        NativeGeometryType::Point(point) => write_kml_point(output, point, precision, prefix),
        NativeGeometryType::LineString(line) => {
            write_kml_line(output, line, "LineString", precision, prefix)
        }
        NativeGeometryType::Polygon(polygon) => {
            write_kml_polygon(output, polygon, precision, prefix)
        }
        NativeGeometryType::MultiPoint(_)
        | NativeGeometryType::MultiLineString(_)
        | NativeGeometryType::MultiPolygon(_) => {
            write_kml_collection(output, geometry, precision, prefix)
        }
        NativeGeometryType::GeometryCollection(_) => Err(DataFusionError::Execution(
            "ST_AsKML: GeometryCollection is not supported by KML output".to_string(),
        )),
        NativeGeometryType::Rect(_)
        | NativeGeometryType::Triangle(_)
        | NativeGeometryType::Line(_) => Err(DataFusionError::Execution(
            "ST_AsKML: unsupported geometry type".to_string(),
        )),
    }
}

fn write_kml_point(
    output: &mut String,
    point: &impl PointTrait<T = f64>,
    precision: i32,
    prefix: &str,
) -> Result<()> {
    let Some(coord) = point.coord() else {
        return Ok(());
    };
    output.push('<');
    output.push_str(prefix);
    output.push_str("Point><");
    output.push_str(prefix);
    output.push_str("coordinates>");
    write_kml_coordinates(output, std::iter::once(coord), precision);
    output.push_str("</");
    output.push_str(prefix);
    output.push_str("coordinates></");
    output.push_str(prefix);
    output.push_str("Point>");
    Ok(())
}

fn write_kml_line(
    output: &mut String,
    line: &impl LineStringTrait<T = f64>,
    name: &str,
    precision: i32,
    prefix: &str,
) -> Result<()> {
    output.push('<');
    output.push_str(prefix);
    output.push_str(name);
    output.push_str("><");
    output.push_str(prefix);
    output.push_str("coordinates>");
    write_kml_coordinates(output, line.coords(), precision);
    output.push_str("</");
    output.push_str(prefix);
    output.push_str("coordinates></");
    output.push_str(prefix);
    output.push_str(name);
    output.push('>');
    Ok(())
}

fn write_kml_polygon(
    output: &mut String,
    polygon: &impl PolygonTrait<T = f64>,
    precision: i32,
    prefix: &str,
) -> Result<()> {
    let Some(exterior) = polygon.exterior() else {
        return Ok(());
    };
    output.push('<');
    output.push_str(prefix);
    output.push_str("Polygon>");
    write_kml_ring(output, "outerBoundaryIs", &exterior, precision, prefix)?;
    for interior in polygon.interiors() {
        write_kml_ring(output, "innerBoundaryIs", &interior, precision, prefix)?;
    }
    output.push_str("</");
    output.push_str(prefix);
    output.push_str("Polygon>");
    Ok(())
}

fn write_kml_ring(
    output: &mut String,
    boundary: &str,
    ring: &impl LineStringTrait<T = f64>,
    precision: i32,
    prefix: &str,
) -> Result<()> {
    output.push('<');
    output.push_str(prefix);
    output.push_str(boundary);
    output.push_str("><");
    output.push_str(prefix);
    output.push_str("LinearRing><");
    output.push_str(prefix);
    output.push_str("coordinates>");
    write_kml_coordinates(output, ring.coords(), precision);
    output.push_str("</");
    output.push_str(prefix);
    output.push_str("coordinates></");
    output.push_str(prefix);
    output.push_str("LinearRing></");
    output.push_str(prefix);
    output.push_str(boundary);
    output.push('>');
    Ok(())
}

fn write_kml_collection(
    output: &mut String,
    collection: &impl GeometryTrait<T = f64>,
    precision: i32,
    prefix: &str,
) -> Result<()> {
    output.push('<');
    output.push_str(prefix);
    output.push_str("MultiGeometry>");
    match collection.as_type() {
        NativeGeometryType::MultiPoint(multi_point) => {
            for point in multi_point.points() {
                write_kml_point(output, &point, precision, prefix)?;
            }
        }
        NativeGeometryType::MultiLineString(multi_line) => {
            for line in multi_line.line_strings() {
                write_kml_line(output, &line, "LineString", precision, prefix)?;
            }
        }
        NativeGeometryType::MultiPolygon(multi_polygon) => {
            for polygon in multi_polygon.polygons() {
                write_kml_polygon(output, &polygon, precision, prefix)?;
            }
        }
        _ => {
            return Err(DataFusionError::Execution(
                "ST_AsKML: unsupported collection member".to_string(),
            ));
        }
    }
    output.push_str("</");
    output.push_str(prefix);
    output.push_str("MultiGeometry>");
    Ok(())
}

fn write_kml_coordinates<C: CoordTrait<T = f64>>(
    output: &mut String,
    coordinates: impl Iterator<Item = C>,
    precision: i32,
) {
    for (index, coord) in coordinates.enumerate() {
        if index > 0 {
            output.push(' ');
        }
        output.push_str(&format_ordinate(coord.x(), precision));
        output.push(',');
        output.push_str(&format_ordinate(coord.y(), precision));
        if matches!(
            coord.dim(),
            geo_traits::Dimensions::Xyz | geo_traits::Dimensions::Xyzm
        ) {
            output.push(',');
            let altitude = coord.nth(2).unwrap_or(f64::NAN);
            output.push_str(&format_ordinate(altitude, precision));
        }
    }
}

fn kml_geometry_is_empty(geometry: &impl GeometryTrait<T = f64>) -> bool {
    match geometry.as_type() {
        NativeGeometryType::Point(point) => point.coord().is_none(),
        NativeGeometryType::LineString(line) => line.num_coords() == 0,
        NativeGeometryType::Polygon(polygon) => polygon.exterior().is_none(),
        NativeGeometryType::MultiPoint(multi_point) => multi_point.num_points() == 0,
        NativeGeometryType::MultiLineString(multi_line) => multi_line.num_line_strings() == 0,
        NativeGeometryType::MultiPolygon(multi_polygon) => multi_polygon.num_polygons() == 0,
        NativeGeometryType::GeometryCollection(collection) => collection.num_geometries() == 0,
        NativeGeometryType::Rect(_)
        | NativeGeometryType::Triangle(_)
        | NativeGeometryType::Line(_) => false,
    }
}

fn parse_kml(input: &str, function_name: &str) -> Result<Wkt<f64>> {
    let root = xml::parse(input, &[KML_NAMESPACE], "KML")?;
    match root.name.as_str() {
        "Point" => Ok(Wkt::Point(parse_kml_point(&root, function_name)?)),
        "LineString" => Ok(Wkt::LineString(parse_kml_line(&root, function_name)?)),
        "Polygon" => Ok(Wkt::Polygon(parse_kml_polygon(&root, function_name)?)),
        "MultiGeometry" => {
            let geometries = parse_kml_multi(&root, function_name)?;
            Ok(homogenize_kml(geometries, function_name)?)
        }
        other => Err(invalid_kml(format!(
            "unexpected KML geometry element {other:?}"
        ))),
    }
}

fn parse_kml_point(node: &XmlNode, function_name: &str) -> Result<Point<f64>> {
    let coords = parse_kml_coordinates(node, function_name)?;
    if coords.len() != 1 {
        return Err(invalid_kml("Point must contain exactly one coordinate"));
    }
    Ok(Point::from_coord(coords[0]))
}

fn parse_kml_line(node: &XmlNode, function_name: &str) -> Result<LineString<f64>> {
    let coords = parse_kml_coordinates(node, function_name)?;
    if coords.len() < 2 {
        return Err(invalid_kml(
            "LineString must contain at least two coordinates",
        ));
    }
    let dim = coords[0].dimension();
    Ok(LineString::new(coords, dim))
}

fn parse_kml_polygon(node: &XmlNode, function_name: &str) -> Result<Polygon<f64>> {
    let mut rings = Vec::new();
    let outer = node
        .children
        .iter()
        .filter(|child| child.name == "outerBoundaryIs")
        .collect::<Vec<_>>();
    if outer.len() != 1 {
        return Err(invalid_kml(
            "Polygon must contain exactly one outerBoundaryIs",
        ));
    }
    rings.push(parse_kml_ring(outer[0], function_name)?);
    for inner in node
        .children
        .iter()
        .filter(|child| child.name == "innerBoundaryIs")
    {
        rings.push(parse_kml_ring(inner, function_name)?);
    }
    let dim = rings[0].dimension();
    if rings.iter().any(|ring| ring.dimension() != dim) {
        return Err(invalid_kml("Polygon rings use mixed coordinate dimensions"));
    }
    Ok(Polygon::new(rings, dim))
}

fn parse_kml_ring(boundary: &XmlNode, function_name: &str) -> Result<LineString<f64>> {
    let ring = boundary
        .children
        .iter()
        .find(|child| child.name == "LinearRing")
        .ok_or_else(|| invalid_kml("Polygon boundary is missing LinearRing"))?;
    let mut coords = parse_kml_coordinates(ring, function_name)?;
    if coords.len() < 4 {
        return Err(invalid_kml(
            "LinearRing must contain at least four coordinates",
        ));
    }
    if coords.first() != coords.last() {
        coords.push(coords[0]);
    }
    let dim = coords[0].dimension();
    Ok(LineString::new(coords, dim))
}

fn parse_kml_multi(node: &XmlNode, function_name: &str) -> Result<Vec<Wkt<f64>>> {
    let mut geometries = Vec::new();
    for child in &node.children {
        match child.name.as_str() {
            "Point" => geometries.push(Wkt::Point(parse_kml_point(child, function_name)?)),
            "LineString" => {
                geometries.push(Wkt::LineString(parse_kml_line(child, function_name)?));
            }
            "Polygon" => geometries.push(Wkt::Polygon(parse_kml_polygon(child, function_name)?)),
            "MultiGeometry" => {
                geometries.extend(parse_kml_multi(child, function_name)?);
            }
            _ => {}
        }
    }
    Ok(geometries)
}

fn homogenize_kml(geometries: Vec<Wkt<f64>>, function_name: &str) -> Result<Wkt<f64>> {
    if geometries.is_empty() {
        return Ok(Wkt::GeometryCollection(GeometryCollection::empty(
            // PostGIS creates the empty KML collection with the Z flag set.
            WktDimension::XYZ,
        )));
    }
    let dim = wkt_dimension(&geometries[0]);
    if geometries
        .iter()
        .any(|geometry| wkt_dimension(geometry) != dim)
    {
        return Err(DataFusionError::Execution(format!(
            "{function_name}: mixed 2D/3D KML members would drop altitude in PostGIS and are not supported"
        )));
    }

    let all_points = geometries
        .iter()
        .all(|geometry| matches!(geometry, Wkt::Point(_)));
    if all_points {
        let points = geometries
            .into_iter()
            .map(|geometry| match geometry {
                Wkt::Point(point) => Ok(point),
                _ => unreachable!(),
            })
            .collect::<Result<Vec<_>>>()?;
        if points.len() == 1 {
            return Ok(Wkt::Point(points.into_iter().next().unwrap()));
        }
        return Ok(Wkt::MultiPoint(MultiPoint::new(points, dim)));
    }

    let all_lines = geometries
        .iter()
        .all(|geometry| matches!(geometry, Wkt::LineString(_)));
    if all_lines {
        let lines = geometries
            .into_iter()
            .map(|geometry| match geometry {
                Wkt::LineString(line) => Ok(line),
                _ => unreachable!(),
            })
            .collect::<Result<Vec<_>>>()?;
        if lines.len() == 1 {
            return Ok(Wkt::LineString(lines.into_iter().next().unwrap()));
        }
        return Ok(Wkt::MultiLineString(MultiLineString::new(lines, dim)));
    }

    let all_polygons = geometries
        .iter()
        .all(|geometry| matches!(geometry, Wkt::Polygon(_)));
    if all_polygons {
        let polygons = geometries
            .into_iter()
            .map(|geometry| match geometry {
                Wkt::Polygon(polygon) => Ok(polygon),
                _ => unreachable!(),
            })
            .collect::<Result<Vec<_>>>()?;
        if polygons.len() == 1 {
            return Ok(Wkt::Polygon(polygons.into_iter().next().unwrap()));
        }
        return Ok(Wkt::MultiPolygon(MultiPolygon::new(polygons, dim)));
    }

    Ok(Wkt::GeometryCollection(GeometryCollection::new(
        geometries, dim,
    )))
}

fn parse_kml_coordinates(node: &XmlNode, _function_name: &str) -> Result<Vec<Coord<f64>>> {
    let coordinates = node
        .children
        .iter()
        .find(|child| child.name == "coordinates")
        .ok_or_else(|| invalid_kml("geometry is missing coordinates"))?;
    let text = coordinates.trimmed_text();
    if text.is_empty() {
        return Err(invalid_kml("coordinates is empty"));
    }
    let mut coords = Vec::new();
    let mut seen_dim = None;
    for tuple in text.split_whitespace() {
        let values = tuple
            .split(',')
            .map(|value| {
                value
                    .parse::<f64>()
                    .map_err(|_| invalid_kml(format!("invalid KML coordinate {value:?}")))
            })
            .collect::<Result<Vec<_>>>()?;
        let dim = match values.len() {
            2 => WktDimension::XY,
            3 => WktDimension::XYZ,
            _ => {
                return Err(invalid_kml(
                    "KML coordinates must contain two or three values",
                ));
            }
        };
        if let Some(expected) = seen_dim
            && expected != dim
        {
            return Err(invalid_kml("KML coordinates use mixed dimensions"));
        }
        if seen_dim.is_none() {
            seen_dim = Some(dim);
        }
        for value in &values {
            if !value.is_finite() {
                return Err(invalid_kml("non-finite KML coordinate"));
            }
        }
        coords.push(coord_from_values(&values, dim));
    }
    Ok(coords)
}

fn coord_from_values(values: &[f64], dim: WktDimension) -> Coord<f64> {
    Coord {
        x: values[0],
        y: values[1],
        z: matches!(dim, WktDimension::XYZ | WktDimension::XYZM).then(|| values[2]),
        m: None,
    }
}

fn wkt_dimension(geometry: &Wkt<f64>) -> WktDimension {
    geometry.dimension()
}

fn invalid_kml(message: impl std::fmt::Display) -> DataFusionError {
    xml::invalid("KML", message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_kml_point_and_polygon_hole() {
        let point = parse_kml(
            "<Point><coordinates>1,2</coordinates></Point>",
            "ST_GeomFromKML",
        )
        .unwrap();
        assert_eq!(point.to_string(), "POINT(1 2)");
        let polygon = parse_kml(
            r#"<Polygon>
                <outerBoundaryIs><LinearRing><coordinates>0,0 0,10 10,10 10,0 0,0</coordinates></LinearRing></outerBoundaryIs>
                <innerBoundaryIs><LinearRing><coordinates>2,2 2,4 4,4 4,2 2,2</coordinates></LinearRing></innerBoundaryIs>
            </Polygon>"#,
            "ST_GeomFromKML",
        )
        .unwrap();
        assert_eq!(
            polygon.to_string(),
            "POLYGON((0 0,0 10,10 10,10 0,0 0),(2 2,2 4,4 4,4 2,2 2))"
        );
    }

    #[test]
    fn auto_closes_unclosed_kml_ring() {
        let polygon = parse_kml(
            r#"<Polygon><outerBoundaryIs><LinearRing><coordinates>0,0 0,1 1,1 1,0</coordinates></LinearRing></outerBoundaryIs></Polygon>"#,
            "ST_GeomFromKML",
        )
        .unwrap();
        assert_eq!(polygon.to_string(), "POLYGON((0 0,0 1,1 1,1 0,0 0))");
    }

    #[test]
    fn homogenizes_multi_geometry() {
        let geometry = parse_kml(
            r#"<MultiGeometry><Point><coordinates>1,2</coordinates></Point><Point><coordinates>3,4</coordinates></Point></MultiGeometry>"#,
            "ST_GeomFromKML",
        )
        .unwrap();
        assert_eq!(geometry.to_string(), "MULTIPOINT((1 2),(3 4))");
    }

    #[test]
    fn preserves_kml_altitude() {
        let geometry = parse_kml(
            "<LineString><coordinates>1,2,3 4,5,6</coordinates></LineString>",
            "ST_GeomFromKML",
        )
        .unwrap();
        assert_eq!(geometry.to_string(), "LINESTRING Z(1 2 3,4 5 6)");
    }
}

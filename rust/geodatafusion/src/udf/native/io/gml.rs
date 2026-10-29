//! GML 2.1/3.2 geometry parsing and PostGIS-compatible GML output.
//!
//! Authority-order (`urn:ogc:def:crs:EPSG::...`) axis handling uses the
//! explicit CRS property table in [`crate::udf::native::crs`]. Unknown SRIDs
//! error instead of guessing axis order from a code range.

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
use datafusion::scalar::ScalarValue;
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
use crate::udf::native::crs::{CrsAxisOrder, epsg_axis_order};
use crate::udf::native::geometry::{
    StrictGeometryBuilder, field_metadata, geometry_array_from_arrow, metadata_for_srid,
    normalize_srid,
};

const GML_NAMESPACE: &str = "http://www.opengis.net/gml";
const GML32_NAMESPACE: &str = "http://www.opengis.net/gml/3.2";
const DEFAULT_PRECISION: i32 = 15;

#[derive(Debug, Eq, Hash, PartialEq)]
pub struct GeomFromGML {
    coord_type: CoordType,
    aliases: Vec<String>,
}

impl GeomFromGML {
    pub(crate) fn new(coord_type: CoordType) -> Self {
        Self {
            coord_type,
            aliases: vec!["st_gmltosql".to_string()],
        }
    }

    fn invoke(&self, args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let text = TextColumn::from_array(Some(&arrays[0]), None)?;
        let metadata = Arc::new(Metadata::try_from(args.return_field.as_ref())?);
        let target = GeometryType::new(metadata.clone()).with_coord_type(self.coord_type);
        let expected_srid = srid_from_metadata(&metadata, self.name())?;
        let explicit_srid = match args.args.get(1) {
            None => SridArgument::Absent,
            Some(ColumnarValue::Scalar(
                ScalarValue::Int32(None) | ScalarValue::Int64(None) | ScalarValue::Null,
            )) => SridArgument::Null,
            Some(ColumnarValue::Scalar(ScalarValue::Int32(Some(value)))) => {
                SridArgument::Value(normalize_srid(i64::from(*value))?)
            }
            Some(ColumnarValue::Scalar(ScalarValue::Int64(Some(value)))) => {
                SridArgument::Value(normalize_srid(*value)?)
            }
            Some(ColumnarValue::Array(_)) => {
                return Err(DataFusionError::Execution(
                    "ST_GeomFromGML: SRID must be a literal because GeoArrow stores CRS in field metadata"
                        .to_string(),
                )
                .into());
            }
            Some(ColumnarValue::Scalar(value)) => {
                return Err(DataFusionError::Internal(format!(
                    "ST_GeomFromGML: unsupported SRID argument {value:?}"
                ))
                .into());
            }
        };

        let mut builder = StrictGeometryBuilder::new(target);
        for row in 0..arrays[0].len() {
            if matches!(explicit_srid, SridArgument::Null) {
                builder.push_geometry(None::<&Wkt<f64>>)?;
                continue;
            }
            let Some(value) = text.value(row) else {
                builder.push_geometry(None::<&Wkt<f64>>)?;
                continue;
            };

            let parsed = parse_gml(value, self.name())?;
            let output_srid = match explicit_srid {
                SridArgument::Absent => {
                    if parsed.srid != expected_srid {
                        return Err(DataFusionError::Execution(format!(
                            "{}: GML row {row} has SRID {} but the output field declares SRID {expected_srid}",
                            self.name(),
                            parsed.srid
                        ))
                        .into());
                    }
                    parsed.srid
                }
                SridArgument::Value(value) => value,
                SridArgument::Null => unreachable!(),
            };
            if output_srid != expected_srid && !matches!(explicit_srid, SridArgument::Value(_)) {
                return Err(DataFusionError::Execution(format!(
                    "{}: row {row} cannot be represented with the declared CRS",
                    self.name()
                ))
                .into());
            }
            builder.push_geometry(Some(&parsed.geometry))?;
        }

        Ok(ColumnarValue::Array(builder.finish().to_array_ref()))
    }
}

impl Default for GeomFromGML {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static GEOM_FROM_GML_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for GeomFromGML {
    fn name(&self) -> &str {
        "st_geomfromgml"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: OnceLock<Signature> = OnceLock::new();
        SIGNATURE.get_or_init(gml_input_signature)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<Arc<Field>> {
        let input = &args.arg_fields[0];
        let metadata = gml_output_metadata(&args, input, self.name())?;
        let typ = GeometryType::new(metadata).with_coord_type(self.coord_type);
        Ok(typ.to_field(input.name(), input.is_nullable()).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.invoke(args).map_err(Into::into)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(GEOM_FROM_GML_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Constructs a geometry from a GML geometry fragment.",
                "ST_GeomFromGML(gml[, srid])",
            )
            .with_argument("gml", "GML geometry fragment")
            .with_argument("srid", "optional literal SRID")
            .build()
        }))
    }
}

fn gml_input_signature() -> Signature {
    let text_types = vec![DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View];
    let mut signatures = vec![TypeSignature::Uniform(1, text_types.clone())];
    for text_type in text_types {
        signatures.push(TypeSignature::Exact(vec![
            text_type.clone(),
            DataType::Int32,
        ]));
        signatures.push(TypeSignature::Exact(vec![text_type, DataType::Int64]));
    }
    Signature::one_of(signatures, Volatility::Immutable)
}

fn gml_output_metadata(
    args: &ReturnFieldArgs<'_>,
    input: &Field,
    function_name: &str,
) -> Result<Arc<Metadata>> {
    if args.arg_fields.len() > 1 {
        return match args.scalar_arguments.get(1) {
            Some(Some(ScalarValue::Int32(Some(value)))) => {
                Ok(metadata_for_srid(normalize_srid(i64::from(*value))?))
            }
            Some(Some(ScalarValue::Int64(Some(value)))) => {
                Ok(metadata_for_srid(normalize_srid(*value)?))
            }
            Some(Some(ScalarValue::Int32(None) | ScalarValue::Int64(None) | ScalarValue::Null))
            | Some(None) => Ok(metadata_for_srid(0)),
            Some(Some(value)) => Err(DataFusionError::Internal(format!(
                "{function_name}: unsupported SRID value {value:?}"
            ))),
            None => Ok(field_metadata(input)?),
        };
    }

    match args.scalar_arguments.first().copied().flatten() {
        Some(ScalarValue::Utf8(Some(value)))
        | Some(ScalarValue::LargeUtf8(Some(value)))
        | Some(ScalarValue::Utf8View(Some(value))) => {
            Ok(metadata_for_srid(parse_gml(value, function_name)?.srid))
        }
        Some(ScalarValue::Utf8(None))
        | Some(ScalarValue::LargeUtf8(None))
        | Some(ScalarValue::Utf8View(None))
        | Some(ScalarValue::Null) => Ok(metadata_for_srid(0)),
        _ => {
            let metadata = field_metadata(input)?;
            srid_from_metadata(&metadata, function_name)?;
            Ok(metadata)
        }
    }
}

#[derive(Debug, Eq, Hash, PartialEq)]
pub struct AsGML {
    signature: Signature,
}

impl AsGML {
    pub(crate) fn new() -> Self {
        Self {
            signature: as_gml_signature(),
        }
    }
}

impl Default for AsGML {
    fn default() -> Self {
        Self::new()
    }
}

static AS_GML_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for AsGML {
    fn name(&self) -> &str {
        "st_asgml"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<Arc<Field>> {
        let geometry_index = gml_geometry_index(args.arg_fields)?;
        let input = &args.arg_fields[geometry_index];
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(input.name(), DataType::Utf8, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let geometry_index = gml_geometry_index(&args.arg_fields)?;
        let version = if geometry_index == 1 {
            IntColumn::from_array(Some(&arrays[0]), None)?
        } else {
            IntColumn::Default(Some(2))
        };
        let precision = IntColumn::from_array(
            arrays.get(geometry_index + 1),
            Some(i64::from(DEFAULT_PRECISION)),
        )?;
        let options = IntColumn::from_array(arrays.get(geometry_index + 2), Some(0))?;
        let prefix = TextColumn::from_array(arrays.get(geometry_index + 3), Some("gml"))?;
        let id = TextColumn::from_array(arrays.get(geometry_index + 4), None)?;

        let geometry_field = &args.arg_fields[geometry_index];
        let metadata = Arc::new(Metadata::try_from(geometry_field.as_ref())?);
        let srid = srid_from_metadata(&metadata, self.name())?;
        let geo_array = geometry_array_from_arrow(&arrays[geometry_index], geometry_field)?;
        let geo_array = geo_array.as_ref();
        let result = downcast_geoarrow_array!(
            geo_array,
            write_gml_array,
            &version,
            &precision,
            &options,
            &prefix,
            &id,
            srid
        )?;
        Ok(ColumnarValue::Array(Arc::new(result)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(AS_GML_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a geometry as a GML 2 or GML 3 geometry element.",
                "ST_AsGML([version,] geometry[, maxdecimaldigits[, options[, nprefix[, id]]]])",
            )
            .with_argument("version", "optional GML version 2 or 3")
            .with_argument("geometry", "input geometry")
            .with_argument("maxdecimaldigits", "maximum decimal digits")
            .with_argument("options", "GML output option bitfield")
            .with_argument("nprefix", "namespace prefix")
            .with_argument("id", "GML 3 geometry identifier")
            .build()
        }))
    }
}

fn gml_geometry_index(fields: &[Arc<Field>]) -> Result<usize> {
    match fields.first().map(|field| field.data_type()) {
        Some(DataType::Int32 | DataType::Int64) => Ok(1),
        Some(_) => Ok(0),
        None => Err(DataFusionError::Internal(
            "ST_AsGML requires at least one argument".to_string(),
        )),
    }
}

fn as_gml_signature() -> Signature {
    let geometry_types = any_geometry_type();
    let integer_types = [DataType::Int32, DataType::Int64];
    let text_types = [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View];
    let mut signatures = Vec::new();

    for geometry_type in &geometry_types {
        signatures.push(TypeSignature::Exact(vec![geometry_type.clone()]));
        for precision_type in &integer_types {
            signatures.push(TypeSignature::Exact(vec![
                geometry_type.clone(),
                precision_type.clone(),
            ]));
            for options_type in &integer_types {
                signatures.push(TypeSignature::Exact(vec![
                    geometry_type.clone(),
                    precision_type.clone(),
                    options_type.clone(),
                ]));
            }
        }
    }

    for version_type in &integer_types {
        for geometry_type in &geometry_types {
            signatures.push(TypeSignature::Exact(vec![
                version_type.clone(),
                geometry_type.clone(),
            ]));
            for precision_type in &integer_types {
                signatures.push(TypeSignature::Exact(vec![
                    version_type.clone(),
                    geometry_type.clone(),
                    precision_type.clone(),
                ]));
                for options_type in &integer_types {
                    signatures.push(TypeSignature::Exact(vec![
                        version_type.clone(),
                        geometry_type.clone(),
                        precision_type.clone(),
                        options_type.clone(),
                    ]));
                    for prefix_type in &text_types {
                        signatures.push(TypeSignature::Exact(vec![
                            version_type.clone(),
                            geometry_type.clone(),
                            precision_type.clone(),
                            options_type.clone(),
                            prefix_type.clone(),
                        ]));
                        for id_type in &text_types {
                            signatures.push(TypeSignature::Exact(vec![
                                version_type.clone(),
                                geometry_type.clone(),
                                precision_type.clone(),
                                options_type.clone(),
                                prefix_type.clone(),
                                id_type.clone(),
                            ]));
                        }
                    }
                }
            }
        }
    }

    Signature::one_of(signatures, Volatility::Immutable)
}

fn write_gml_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    version: &IntColumn<'_>,
    precision: &IntColumn<'_>,
    options: &IntColumn<'_>,
    prefix: &TextColumn<'_>,
    id: &TextColumn<'_>,
    srid: i32,
) -> GeoDataFusionResult<StringArray> {
    let mut builder = StringBuilder::with_capacity(array.len(), array.len() * 128);
    for (row, item) in array.iter().enumerate() {
        let Some(version) = version.value(row) else {
            return Err(DataFusionError::Execution(
                "ST_AsGML: version must not be NULL".to_string(),
            )
            .into());
        };
        if !matches!(version, 2 | 3) {
            return Err(DataFusionError::Execution(
                "ST_AsGML: only GML 2 and GML 3 are supported".to_string(),
            )
            .into());
        }
        let precision = precision.value(row).unwrap_or(i64::from(DEFAULT_PRECISION));
        let options = options.value(row).unwrap_or(0);
        let prefix = prefix.value(row).unwrap_or("gml");
        let id = id.value(row);

        match item {
            None => builder.append_null(),
            Some(Err(error)) => return Err(error.into()),
            Some(Ok(geometry)) => {
                let options = GmlWriteOptions {
                    version: version as u8,
                    precision: i32::try_from(precision).unwrap_or(if precision < 0 {
                        i32::MIN
                    } else {
                        i32::MAX
                    }),
                    options: i32::try_from(options).unwrap_or(if options < 0 {
                        i32::MIN
                    } else {
                        i32::MAX
                    }),
                    prefix: normalize_prefix(prefix)?,
                    id: id.map(str::to_string),
                    srid,
                };
                match write_gml(&geometry, &options)? {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
        }
    }
    Ok(builder.finish())
}

#[derive(Clone)]
struct GmlWriteOptions {
    version: u8,
    precision: i32,
    options: i32,
    prefix: String,
    id: Option<String>,
    srid: i32,
}

impl GmlWriteOptions {
    fn long_crs(&self) -> bool {
        self.options & 1 != 0
    }

    fn omit_dimension(&self) -> bool {
        self.options & 2 != 0
    }

    fn short_line(&self) -> bool {
        self.options & 4 != 0
    }

    fn reverse_axis(&self) -> bool {
        self.version == 3 && self.options & 16 != 0
    }

    fn envelope(&self) -> bool {
        self.options & 32 != 0
    }
}

fn normalize_prefix(prefix: &str) -> GeoDataFusionResult<String> {
    if prefix.is_empty() {
        return Ok(String::new());
    }
    let mut characters = prefix.chars();
    let valid_first = characters
        .next()
        .is_some_and(|character| character == '_' || character.is_ascii_alphabetic());
    let valid_rest = characters.all(|character| {
        character == '_'
            || character == '-'
            || character == '.'
            || character.is_ascii_alphanumeric()
    });
    if !valid_first || !valid_rest || prefix.contains(':') {
        return Err(DataFusionError::Execution(format!(
            "ST_AsGML: invalid XML namespace prefix {prefix:?}"
        ))
        .into());
    }
    Ok(format!("{prefix}:"))
}

#[derive(Clone, Copy)]
enum SridArgument {
    Absent,
    Null,
    Value(i32),
}

#[derive(Clone, Copy, Debug)]
struct GmlSrs {
    srid: i32,
    reverse_axis: bool,
}

#[derive(Debug)]
struct ParsedGml {
    geometry: Wkt<f64>,
    srid: i32,
}

#[derive(Default)]
struct ParseContext {
    root_srid: Option<i32>,
}

fn parse_gml(input: &str, function_name: &str) -> Result<ParsedGml> {
    let root = xml::parse(input, &[GML_NAMESPACE, GML32_NAMESPACE], "GML")?;
    let mut context = ParseContext::default();
    let geometry = parse_geometry_node(&root, None, None, &mut context, function_name)?;
    Ok(ParsedGml {
        geometry,
        srid: context.root_srid.unwrap_or(0),
    })
}

fn parse_geometry_node(
    node: &XmlNode,
    inherited_srs: Option<GmlSrs>,
    inherited_dim: Option<WktDimension>,
    context: &mut ParseContext,
    function_name: &str,
) -> Result<Wkt<f64>> {
    let srs = node
        .attribute("srsName")
        .map(|value| parse_srs_name(value, function_name))
        .transpose()?
        .or(inherited_srs);
    if let Some(srs) = srs {
        context.observe(srs, function_name)?;
    }
    let dim = dimension_from_node(node)?.or(inherited_dim);

    match node.name.as_str() {
        "Point" => parse_point(node, srs, dim, context, function_name),
        "LineString" | "LinearRing" => parse_line(node, srs, dim, context, function_name),
        "Curve" => parse_curve(node, srs, dim, context, function_name),
        "Polygon" => parse_polygon(node, srs, dim, context, function_name),
        "MultiPoint" => parse_multi_point(node, srs, dim, context, function_name),
        "MultiLineString" | "MultiCurve" => {
            parse_multi_line(node, srs, dim, context, function_name)
        }
        "MultiPolygon" | "MultiSurface" => {
            parse_multi_polygon(node, srs, dim, context, function_name)
        }
        "MultiGeometry" | "GeometryCollection" => {
            parse_geometry_collection(node, srs, dim, context, function_name)
        }
        unsupported => Err(unsupported_geometry(unsupported, function_name)),
    }
}

impl ParseContext {
    fn observe(&mut self, srs: GmlSrs, function_name: &str) -> Result<()> {
        match self.root_srid {
            Some(srid) if srid != srs.srid => Err(DataFusionError::Execution(format!(
                "{function_name}: mixed GML SRIDs {srid} and {} are not representable by one GeoArrow field",
                srs.srid
            ))),
            Some(_) => Ok(()),
            None => {
                self.root_srid = Some(srs.srid);
                Ok(())
            }
        }
    }
}

fn parse_point(
    node: &XmlNode,
    srs: Option<GmlSrs>,
    dim: Option<WktDimension>,
    context: &mut ParseContext,
    function_name: &str,
) -> Result<Wkt<f64>> {
    let mut coords = parse_coordinate_data(node, dim, context, function_name)?;
    if coords.is_empty() {
        return Ok(Wkt::Point(Point::empty(dim.unwrap_or(WktDimension::XY))));
    }
    if coords.len() != 1 {
        return Err(invalid_gml("Point contains more than one coordinate"));
    }
    if let Some(srs) = srs
        && srs.reverse_axis
    {
        flip_coord(&mut coords[0]);
    }
    Ok(Wkt::Point(Point::from_coord(coords.remove(0))))
}

fn parse_line(
    node: &XmlNode,
    srs: Option<GmlSrs>,
    dim: Option<WktDimension>,
    context: &mut ParseContext,
    function_name: &str,
) -> Result<Wkt<f64>> {
    let mut coords = parse_coordinate_data(node, dim, context, function_name)?;
    if coords.is_empty() {
        return Ok(Wkt::LineString(LineString::empty(
            dim.unwrap_or(WktDimension::XY),
        )));
    }
    if coords.len() < 2 {
        return Err(invalid_gml(
            "LineString must contain at least two coordinates",
        ));
    }
    let actual_dim = coords[0].dimension();
    ensure_same_dimension(&coords, actual_dim)?;
    if let Some(srs) = srs
        && srs.reverse_axis
    {
        for coord in &mut coords {
            flip_coord(coord);
        }
    }
    Ok(Wkt::LineString(LineString::new(coords, actual_dim)))
}

fn parse_curve(
    node: &XmlNode,
    srs: Option<GmlSrs>,
    dim: Option<WktDimension>,
    context: &mut ParseContext,
    function_name: &str,
) -> Result<Wkt<f64>> {
    let Some(segments) = node.child("segments") else {
        return Ok(Wkt::LineString(LineString::empty(
            dim.unwrap_or(WktDimension::XY),
        )));
    };

    let mut all = Vec::new();
    for segment in segments
        .children
        .iter()
        .filter(|child| child.name == "LineStringSegment")
    {
        if let Some(interpolation) = segment.attribute("interpolation")
            && interpolation != "linear"
        {
            return Err(unsupported_gml(
                format!("Curve interpolation {interpolation:?}"),
                function_name,
            ));
        }
        let mut part = parse_coordinate_data(segment, dim, context, function_name)?;
        if part.len() < 2 {
            return Err(invalid_gml(
                "LineStringSegment must contain at least two coordinates",
            ));
        }
        if let Some(last) = all.last()
            && last != &part[0]
        {
            return Err(invalid_gml("adjacent GML curve segments are not connected"));
        }
        if all.is_empty() {
            all.append(&mut part);
        } else {
            part.remove(0);
            all.append(&mut part);
        }
    }

    if all.is_empty() {
        return Ok(Wkt::LineString(LineString::empty(
            dim.unwrap_or(WktDimension::XY),
        )));
    }
    let actual_dim = all[0].dimension();
    ensure_same_dimension(&all, actual_dim)?;
    if let Some(srs) = srs
        && srs.reverse_axis
    {
        for coord in &mut all {
            flip_coord(coord);
        }
    }
    Ok(Wkt::LineString(LineString::new(all, actual_dim)))
}

fn parse_polygon(
    node: &XmlNode,
    srs: Option<GmlSrs>,
    dim: Option<WktDimension>,
    context: &mut ParseContext,
    function_name: &str,
) -> Result<Wkt<f64>> {
    let exterior = node
        .child("exterior")
        .or_else(|| node.child("outerBoundaryIs"));
    let interiors = node
        .children
        .iter()
        .filter(|child| child.name == "interior" || child.name == "innerBoundaryIs")
        .collect::<Vec<_>>();

    if exterior.is_none() && interiors.is_empty() {
        return Ok(Wkt::Polygon(Polygon::empty(
            dim.unwrap_or(WktDimension::XY),
        )));
    }
    let Some(exterior) = exterior else {
        return Err(invalid_gml(
            "Polygon has interior rings without an exterior",
        ));
    };

    let mut rings = Vec::new();
    rings.push(parse_boundary_ring(
        exterior,
        srs,
        dim,
        context,
        function_name,
    )?);
    for interior in interiors {
        rings.push(parse_boundary_ring(
            interior,
            srs,
            dim,
            context,
            function_name,
        )?);
    }
    let actual_dim = rings[0].dimension();
    if rings.iter().any(|ring| ring.dimension() != actual_dim) {
        return Err(invalid_gml("Polygon rings use mixed coordinate dimensions"));
    }
    Ok(Wkt::Polygon(Polygon::new(rings, actual_dim)))
}

fn parse_boundary_ring(
    boundary: &XmlNode,
    srs: Option<GmlSrs>,
    dim: Option<WktDimension>,
    context: &mut ParseContext,
    function_name: &str,
) -> Result<LineString<f64>> {
    let ring = boundary
        .children
        .iter()
        .find(|child| child.name == "LinearRing")
        .ok_or_else(|| invalid_gml("Polygon boundary is missing LinearRing"))?;
    let ring_dim = dimension_from_node(ring)?.or(dim);
    let mut coords = parse_coordinate_data(ring, ring_dim, context, function_name)?;
    validate_ring(&coords)?;
    let actual_dim = coords[0].dimension();
    if let Some(srs) = srs
        && srs.reverse_axis
    {
        for coord in &mut coords {
            flip_coord(coord);
        }
    }
    Ok(LineString::new(coords, actual_dim))
}

fn parse_multi_point(
    node: &XmlNode,
    srs: Option<GmlSrs>,
    dim: Option<WktDimension>,
    context: &mut ParseContext,
    function_name: &str,
) -> Result<Wkt<f64>> {
    let members = geometry_members(node, &["pointMember"], &["pointMembers"])?;
    let mut points = Vec::new();
    for member in members {
        let geometry = parse_geometry_node(&member, srs, dim, context, function_name)?;
        let Wkt::Point(point) = geometry else {
            return Err(invalid_gml("MultiPoint contains a non-Point member"));
        };
        if point.coord().is_none() {
            return Err(invalid_gml(
                "MultiPoint cannot contain an empty Point member in GeoArrow",
            ));
        }
        points.push(point);
    }
    let actual_dim = points
        .first()
        .map(Point::dimension)
        .unwrap_or(dim.unwrap_or(WktDimension::XY));
    if points.iter().any(|point| point.dimension() != actual_dim) {
        return Err(invalid_gml(
            "MultiPoint members use mixed coordinate dimensions",
        ));
    }
    Ok(Wkt::MultiPoint(MultiPoint::new(points, actual_dim)))
}

fn parse_multi_line(
    node: &XmlNode,
    srs: Option<GmlSrs>,
    dim: Option<WktDimension>,
    context: &mut ParseContext,
    function_name: &str,
) -> Result<Wkt<f64>> {
    let members = geometry_members(
        node,
        &["lineStringMember", "curveMember"],
        &["lineStringMembers", "curveMembers"],
    )?;
    let mut lines = Vec::new();
    for member in members {
        let geometry = parse_geometry_node(&member, srs, dim, context, function_name)?;
        match geometry {
            Wkt::LineString(line) => lines.push(line),
            _ => {
                return Err(invalid_gml(
                    "multi-line geometry contains a non-LineString member",
                ));
            }
        }
    }
    let actual_dim = lines
        .first()
        .map(LineString::dimension)
        .unwrap_or(dim.unwrap_or(WktDimension::XY));
    if lines.iter().any(|line| line.dimension() != actual_dim) {
        return Err(invalid_gml(
            "multi-line members use mixed coordinate dimensions",
        ));
    }
    Ok(Wkt::MultiLineString(MultiLineString::new(
        lines, actual_dim,
    )))
}

fn parse_multi_polygon(
    node: &XmlNode,
    srs: Option<GmlSrs>,
    dim: Option<WktDimension>,
    context: &mut ParseContext,
    function_name: &str,
) -> Result<Wkt<f64>> {
    let members = geometry_members(
        node,
        &["polygonMember", "surfaceMember"],
        &["polygonMembers", "surfaceMembers"],
    )?;
    let mut polygons = Vec::new();
    for member in members {
        let geometry = parse_geometry_node(&member, srs, dim, context, function_name)?;
        let Wkt::Polygon(polygon) = geometry else {
            return Err(invalid_gml(
                "multi-surface geometry contains a non-Polygon member",
            ));
        };
        if polygon.exterior().is_none() {
            return Err(invalid_gml(
                "MultiPolygon cannot contain an empty Polygon member in GeoArrow",
            ));
        }
        polygons.push(polygon);
    }
    let actual_dim = polygons
        .first()
        .map(Polygon::dimension)
        .unwrap_or(dim.unwrap_or(WktDimension::XY));
    if polygons
        .iter()
        .any(|polygon| polygon.dimension() != actual_dim)
    {
        return Err(invalid_gml(
            "multi-polygon members use mixed coordinate dimensions",
        ));
    }
    Ok(Wkt::MultiPolygon(MultiPolygon::new(polygons, actual_dim)))
}

fn parse_geometry_collection(
    node: &XmlNode,
    srs: Option<GmlSrs>,
    dim: Option<WktDimension>,
    context: &mut ParseContext,
    function_name: &str,
) -> Result<Wkt<f64>> {
    let members = geometry_members(node, &["geometryMember"], &["geometryMembers"])?;
    let mut geometries = Vec::new();
    for member in members {
        geometries.push(parse_geometry_node(
            &member,
            srs,
            dim,
            context,
            function_name,
        )?);
    }
    let actual_dim = geometries
        .first()
        .map(wkt_dimension)
        .unwrap_or(dim.unwrap_or(WktDimension::XY));
    if geometries
        .iter()
        .any(|geometry| wkt_dimension(geometry) != actual_dim)
    {
        return Err(invalid_gml(
            "GeometryCollection members use mixed coordinate dimensions",
        ));
    }
    Ok(Wkt::GeometryCollection(GeometryCollection::new(
        geometries, actual_dim,
    )))
}

fn geometry_members(
    node: &XmlNode,
    member_names: &[&str],
    container_names: &[&str],
) -> Result<Vec<XmlNode>> {
    let mut members = Vec::new();
    for child in &node.children {
        if member_names.contains(&child.name.as_str()) {
            members.push(find_geometry_child(child)?.clone());
        } else if container_names.contains(&child.name.as_str()) {
            for candidate in &child.children {
                if is_geometry_element(candidate) {
                    members.push(candidate.clone());
                }
            }
        }
    }
    Ok(members)
}

fn find_geometry_child(node: &XmlNode) -> Result<&XmlNode> {
    node.children
        .iter()
        .find(|child| is_geometry_element(child))
        .ok_or_else(|| invalid_gml(format!("{} has no geometry child", node.name)))
}

fn is_geometry_element(node: &XmlNode) -> bool {
    matches!(
        node.name.as_str(),
        "Point"
            | "LineString"
            | "LinearRing"
            | "Curve"
            | "Polygon"
            | "MultiPoint"
            | "MultiLineString"
            | "MultiCurve"
            | "MultiPolygon"
            | "MultiSurface"
            | "MultiGeometry"
            | "GeometryCollection"
    )
}

fn parse_coordinate_data(
    node: &XmlNode,
    dim: Option<WktDimension>,
    context: &mut ParseContext,
    function_name: &str,
) -> Result<Vec<Coord<f64>>> {
    let mut coords = Vec::new();
    let mut actual_dim = dim;
    for child in &node.children {
        let mut parsed = match child.name.as_str() {
            "pos" => {
                let coord_dim = dimension_from_node(child)?
                    .or(dim)
                    .unwrap_or(WktDimension::XY);
                vec![parse_pos(child, coord_dim)?]
            }
            "posList" => {
                let coord_dim = dimension_from_node(child)?
                    .or(dim)
                    .unwrap_or(WktDimension::XY);
                parse_pos_list(child, coord_dim)?
            }
            "coordinates" => parse_gml2_coordinates(child, dim)?,
            "coord" => vec![parse_gml2_coord(child, dim)?],
            "pointProperty" | "pointRep" => {
                let point = find_geometry_child(child)?;
                let Wkt::Point(point) =
                    parse_geometry_node(point, None, dim, context, function_name)?
                else {
                    return Err(invalid_gml("pointProperty does not contain a Point"));
                };
                point.coord().cloned().into_iter().collect()
            }
            _ => continue,
        };
        if !parsed.is_empty() {
            let this_dim = parsed[0].dimension();
            if let Some(expected_dim) = actual_dim
                && this_dim != expected_dim
            {
                return Err(invalid_gml(
                    "coordinate elements use inconsistent dimensions",
                ));
            }
            if actual_dim.is_none() {
                actual_dim = Some(this_dim);
            }
            coords.append(&mut parsed);
        }
    }
    if let Some(expected_dim) = dim
        && coords.iter().any(|coord| coord.dimension() != expected_dim)
    {
        return Err(invalid_gml("coordinate values do not match srsDimension"));
    }
    Ok(coords)
}

fn parse_pos(node: &XmlNode, dim: WktDimension) -> Result<Coord<f64>> {
    let values = parse_number_tokens(node.trimmed_text(), "gml:pos")?;
    if values.len() != dimension_len(dim) {
        return Err(invalid_gml(format!(
            "gml:pos contains {} values but srsDimension is {}",
            values.len(),
            dimension_len(dim)
        )));
    }
    Ok(coord_from_values(&values, dim))
}

fn parse_pos_list(node: &XmlNode, dim: WktDimension) -> Result<Vec<Coord<f64>>> {
    let values = parse_number_tokens(node.trimmed_text(), "gml:posList")?;
    let dimension_len = dimension_len(dim);
    if values.len() % dimension_len != 0 {
        return Err(invalid_gml(format!(
            "gml:posList has {} values, which is not divisible by srsDimension {}",
            values.len(),
            dimension_len
        )));
    }
    Ok(values
        .chunks_exact(dimension_len)
        .map(|values| coord_from_values(values, dim))
        .collect())
}

fn parse_gml2_coord(node: &XmlNode, hint: Option<WktDimension>) -> Result<Coord<f64>> {
    let x = parse_number(
        node.child("X")
            .ok_or_else(|| invalid_gml("gml:coord is missing X"))?
            .trimmed_text(),
    )?;
    let y = parse_number(
        node.child("Y")
            .ok_or_else(|| invalid_gml("gml:coord is missing Y"))?
            .trimmed_text(),
    )?;
    let z = node
        .child("Z")
        .map(|child| parse_number(child.trimmed_text()))
        .transpose()?;
    let m = node
        .child("M")
        .map(|child| parse_number(child.trimmed_text()))
        .transpose()?;
    let dim = match (z, m) {
        (Some(_), Some(_)) => WktDimension::XYZM,
        (Some(_), None) => WktDimension::XYZ,
        (None, Some(_)) => WktDimension::XYM,
        (None, None) => WktDimension::XY,
    };
    if let Some(hint) = hint
        && hint != dim
    {
        return Err(invalid_gml("gml:coord does not match srsDimension"));
    }
    Ok(Coord { x, y, z, m })
}

fn parse_gml2_coordinates(node: &XmlNode, hint: Option<WktDimension>) -> Result<Vec<Coord<f64>>> {
    let text = node.trimmed_text();
    if text.is_empty() {
        return Ok(Vec::new());
    }
    let tuple_separator = node.attribute("ts").unwrap_or(" ");
    let coordinate_separator = node.attribute("cs").unwrap_or(",");
    let decimal = node.attribute("decimal").unwrap_or(".");
    if tuple_separator == coordinate_separator
        || tuple_separator == decimal
        || coordinate_separator == decimal
    {
        return Err(invalid_gml("gml:coordinates separators are not distinct"));
    }
    if tuple_separator.chars().count() > 1
        || coordinate_separator.chars().count() > 1
        || decimal.chars().count() > 1
    {
        return Err(invalid_gml(
            "gml:coordinates separators must be one character",
        ));
    }
    let tuple_separator = tuple_separator.chars().next().unwrap();
    let coordinate_separator = coordinate_separator.chars().next().unwrap();
    let decimal = decimal.chars().next().unwrap();
    let tuples = if tuple_separator.is_whitespace() {
        text.split_whitespace().collect::<Vec<_>>()
    } else {
        text.split(tuple_separator)
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
    };
    let mut coords = Vec::new();
    for tuple in tuples {
        let values = tuple
            .split(coordinate_separator)
            .map(|value| {
                let value = if decimal == '.' {
                    value.to_string()
                } else {
                    value.replace(decimal, ".")
                };
                parse_number(&value)
            })
            .collect::<Result<Vec<_>>>()?;
        let dim = match values.len() {
            2 => WktDimension::XY,
            3 => WktDimension::XYZ,
            4 => WktDimension::XYZM,
            _ => {
                return Err(invalid_gml(
                    "gml:coordinates tuples must contain two to four values",
                ));
            }
        };
        if let Some(hint) = hint
            && hint != dim
        {
            return Err(invalid_gml("gml:coordinates does not match srsDimension"));
        }
        coords.push(coord_from_values(&values, dim));
    }
    Ok(coords)
}

fn parse_number_tokens(text: &str, element: &str) -> Result<Vec<f64>> {
    if text.is_empty() {
        return Ok(Vec::new());
    }
    text.split_whitespace()
        .map(|value| {
            if value.contains(',') {
                Err(invalid_gml(format!(
                    "{element} contains an unexpected comma"
                )))
            } else {
                parse_number(value)
            }
        })
        .collect()
}

fn parse_number(value: &str) -> Result<f64> {
    let value = value.trim();
    if value.is_empty() {
        return Err(invalid_gml("empty coordinate value"));
    }
    let parsed = value
        .parse::<f64>()
        .map_err(|_| invalid_gml(format!("invalid coordinate value {value:?}")))?;
    if !parsed.is_finite() {
        return Err(invalid_gml(format!(
            "non-finite coordinate value {value:?}"
        )));
    }
    Ok(parsed)
}

fn coord_from_values(values: &[f64], dim: WktDimension) -> Coord<f64> {
    Coord {
        x: values[0],
        y: values[1],
        z: matches!(dim, WktDimension::XYZ | WktDimension::XYZM).then(|| values[2]),
        m: match dim {
            WktDimension::XYM => Some(values[2]),
            WktDimension::XYZM => Some(values[3]),
            WktDimension::XY | WktDimension::XYZ => None,
        },
    }
}

fn dimension_len(dim: WktDimension) -> usize {
    match dim {
        WktDimension::XY => 2,
        WktDimension::XYZ | WktDimension::XYM => 3,
        WktDimension::XYZM => 4,
    }
}

fn dimension_from_node(node: &XmlNode) -> Result<Option<WktDimension>> {
    let Some(value) = node
        .attribute("srsDimension")
        .or_else(|| node.attribute("dimension"))
    else {
        return Ok(None);
    };
    let value = value
        .trim()
        .parse::<usize>()
        .map_err(|_| invalid_gml(format!("invalid srsDimension {value:?}")))?;
    match value {
        2 => Ok(Some(WktDimension::XY)),
        3 => Ok(Some(WktDimension::XYZ)),
        4 => Ok(Some(WktDimension::XYZM)),
        _ => Err(invalid_gml(format!(
            "unsupported srsDimension {value}; expected 2, 3, or 4"
        ))),
    }
}

fn parse_srs_name(value: &str, function_name: &str) -> Result<GmlSrs> {
    let value = value.trim();
    let (code, honours_authority_order) = if let Some(code) = value.strip_prefix("EPSG:") {
        (code, false)
    } else if let Some(code) = value
        .strip_prefix("urn:ogc:def:crs:EPSG:")
        .or_else(|| value.strip_prefix("urn:x-ogc:def:crs:EPSG:"))
        .or_else(|| value.strip_prefix("urn:EPSG:geographicCRS:"))
    {
        (code.rsplit(':').next().unwrap_or(code), true)
    } else if let Some(code) = value.strip_prefix("http://www.opengis.net/gml/srs/epsg.xml#") {
        (code, false)
    } else if let Some(code) = value.rsplit('/').next() {
        (code, false)
    } else {
        return Err(unknown_srs(function_name, value));
    };

    if code.is_empty() || !code.chars().all(|character| character.is_ascii_digit()) {
        return Err(unknown_srs(function_name, value));
    }
    let srid = normalize_srid(
        code.parse::<i64>()
            .map_err(|_| unknown_srs(function_name, value))?,
    )?;
    if srid <= 0 {
        return Err(unknown_srs(function_name, value));
    }

    let reverse_axis = if honours_authority_order {
        matches!(epsg_axis_order(srid)?, CrsAxisOrder::LatitudeLongitude)
    } else {
        false
    };
    Ok(GmlSrs { srid, reverse_axis })
}

fn unknown_srs(function_name: &str, value: &str) -> DataFusionError {
    DataFusionError::Execution(format!(
        "{function_name}: unknown spatial reference system {value:?}"
    ))
}

fn validate_ring(coords: &[Coord<f64>]) -> Result<()> {
    if coords.len() < 4 {
        return Err(invalid_gml(
            "LinearRing must contain at least four coordinates",
        ));
    }
    if coords.first() != coords.last() {
        return Err(invalid_gml("LinearRing is not closed"));
    }
    Ok(())
}

fn ensure_same_dimension(coords: &[Coord<f64>], dim: WktDimension) -> Result<()> {
    if coords.iter().any(|coord| coord.dimension() != dim) {
        return Err(invalid_gml(
            "coordinate elements use inconsistent dimensions",
        ));
    }
    Ok(())
}

fn flip_coord(coord: &mut Coord<f64>) {
    std::mem::swap(&mut coord.x, &mut coord.y);
}

fn wkt_dimension(geometry: &Wkt<f64>) -> WktDimension {
    geometry.dimension()
}

fn unsupported_geometry(name: &str, function_name: &str) -> DataFusionError {
    unsupported_gml(format!("GML geometry type {name}"), function_name)
}

fn unsupported_gml(what: impl std::fmt::Display, function_name: &str) -> DataFusionError {
    DataFusionError::NotImplemented(format!(
        "{function_name}: {what} cannot be represented by GeoArrow"
    ))
}

fn invalid_gml(message: impl std::fmt::Display) -> DataFusionError {
    xml::invalid("GML", message)
}

fn write_gml(
    geometry: &impl GeometryTrait<T = f64>,
    options: &GmlWriteOptions,
) -> Result<Option<String>> {
    if options.options & 8 != 0 {
        return Err(DataFusionError::Execution(
            "ST_AsGML: options value 8 is not supported".to_string(),
        ));
    }
    if geometry_is_empty(geometry)? {
        return Ok(None);
    }

    let mut output = String::new();
    if options.envelope() {
        write_envelope(&mut output, geometry, options)?;
    } else {
        write_geometry_element(&mut output, geometry, options, true)?;
    }
    Ok(Some(output))
}

fn write_geometry_element(
    output: &mut String,
    geometry: &impl GeometryTrait<T = f64>,
    options: &GmlWriteOptions,
    top_level: bool,
) -> Result<()> {
    match geometry.as_type() {
        NativeGeometryType::Point(point) => write_point(output, point, options, top_level),
        NativeGeometryType::LineString(line) => write_line(output, line, options, top_level),
        NativeGeometryType::Polygon(polygon) => write_polygon(output, polygon, options, top_level),
        NativeGeometryType::MultiPoint(multi_point) => {
            write_multi_point(output, multi_point, options, top_level)
        }
        NativeGeometryType::MultiLineString(multi_line) => {
            write_multi_line(output, multi_line, options, top_level)
        }
        NativeGeometryType::MultiPolygon(multi_polygon) => {
            write_multi_polygon(output, multi_polygon, options, top_level)
        }
        NativeGeometryType::GeometryCollection(collection) => {
            write_geometry_collection(output, collection, options, top_level)
        }
        NativeGeometryType::Rect(_)
        | NativeGeometryType::Triangle(_)
        | NativeGeometryType::Line(_) => Err(unsupported_gml("geometry type", "ST_AsGML")),
    }
}

fn write_point(
    output: &mut String,
    point: &impl PointTrait<T = f64>,
    options: &GmlWriteOptions,
    top_level: bool,
) -> Result<()> {
    let Some(coord) = point.coord() else {
        return write_empty_tag(output, "Point", options, top_level);
    };
    write_start_tag(output, "Point", options, top_level, false);
    if options.version == 2 {
        write_gml2_coordinate_element(output, std::iter::once(coord), options)?;
    } else {
        write_gml3_pos(output, coord, options)?;
    }
    write_close_tag(output, "Point", options);
    Ok(())
}

fn write_line(
    output: &mut String,
    line: &impl LineStringTrait<T = f64>,
    options: &GmlWriteOptions,
    top_level: bool,
) -> Result<()> {
    if line.num_coords() == 0 {
        return write_empty_tag(output, "LineString", options, top_level);
    }
    if options.version == 2 {
        write_start_tag(output, "LineString", options, top_level, false);
        write_gml2_coordinate_element(output, line.coords(), options)?;
        write_close_tag(output, "LineString", options);
    } else if options.short_line() {
        write_start_tag(output, "LineString", options, top_level, false);
        write_gml3_pos_list(output, line.coords(), options)?;
        write_close_tag(output, "LineString", options);
    } else {
        write_start_tag(output, "Curve", options, top_level, false);
        let prefix = &options.prefix;
        output.push('<');
        output.push_str(prefix);
        output.push_str("segments>");
        output.push('<');
        output.push_str(prefix);
        output.push_str("LineStringSegment>");
        write_gml3_pos_list(output, line.coords(), options)?;
        output.push_str("</");
        output.push_str(prefix);
        output.push_str("LineStringSegment></");
        output.push_str(prefix);
        output.push_str("segments>");
        write_close_tag(output, "Curve", options);
    }
    Ok(())
}

fn write_polygon(
    output: &mut String,
    polygon: &impl PolygonTrait<T = f64>,
    options: &GmlWriteOptions,
    top_level: bool,
) -> Result<()> {
    let Some(exterior) = polygon.exterior().filter(|ring| ring.num_coords() > 0) else {
        return write_empty_tag(output, "Polygon", options, top_level);
    };
    write_start_tag(output, "Polygon", options, top_level, false);
    let (exterior_name, interior_name) = if options.version == 2 {
        ("outerBoundaryIs", "innerBoundaryIs")
    } else {
        ("exterior", "interior")
    };
    write_polygon_ring(output, exterior_name, exterior, options)?;
    for interior in polygon.interiors() {
        write_polygon_ring(output, interior_name, interior, options)?;
    }
    write_close_tag(output, "Polygon", options);
    Ok(())
}

fn write_polygon_ring(
    output: &mut String,
    relationship: &str,
    ring: impl LineStringTrait<T = f64>,
    options: &GmlWriteOptions,
) -> Result<()> {
    output.push('<');
    output.push_str(&options.prefix);
    output.push_str(relationship);
    output.push('>');
    output.push('<');
    output.push_str(&options.prefix);
    output.push_str("LinearRing>");
    if options.version == 2 {
        write_gml2_coordinate_element(output, ring.coords(), options)?;
    } else {
        write_gml3_pos_list(output, ring.coords(), options)?;
    }
    output.push_str("</");
    output.push_str(&options.prefix);
    output.push_str("LinearRing></");
    output.push_str(&options.prefix);
    output.push_str(relationship);
    output.push('>');
    Ok(())
}

fn write_multi_point(
    output: &mut String,
    multi_point: &impl MultiPointTrait<T = f64>,
    options: &GmlWriteOptions,
    top_level: bool,
) -> Result<()> {
    if multi_point.num_points() == 0 {
        return write_empty_tag(output, "MultiPoint", options, top_level);
    }
    write_start_tag(output, "MultiPoint", options, top_level, false);
    for point in multi_point.points() {
        let member = "pointMember";
        output.push('<');
        output.push_str(&options.prefix);
        output.push_str(member);
        output.push('>');
        write_point(output, &point, options, false)?;
        output.push_str("</");
        output.push_str(&options.prefix);
        output.push_str(member);
        output.push('>');
    }
    write_close_tag(output, "MultiPoint", options);
    Ok(())
}

fn write_multi_line(
    output: &mut String,
    multi_line: &impl MultiLineStringTrait<T = f64>,
    options: &GmlWriteOptions,
    top_level: bool,
) -> Result<()> {
    let name = if options.version == 2 {
        "MultiLineString"
    } else {
        "MultiCurve"
    };
    if multi_line.num_line_strings() == 0 {
        return write_empty_tag(output, name, options, top_level);
    }
    write_start_tag(output, name, options, top_level, false);
    let member = if options.version == 2 {
        "lineStringMember"
    } else {
        "curveMember"
    };
    for line in multi_line.line_strings() {
        output.push('<');
        output.push_str(&options.prefix);
        output.push_str(member);
        output.push('>');
        write_line(output, &line, options, false)?;
        output.push_str("</");
        output.push_str(&options.prefix);
        output.push_str(member);
        output.push('>');
    }
    write_close_tag(output, name, options);
    Ok(())
}

fn write_multi_polygon(
    output: &mut String,
    multi_polygon: &impl MultiPolygonTrait<T = f64>,
    options: &GmlWriteOptions,
    top_level: bool,
) -> Result<()> {
    let name = if options.version == 2 {
        "MultiPolygon"
    } else {
        "MultiSurface"
    };
    if multi_polygon.num_polygons() == 0 {
        return write_empty_tag(output, name, options, top_level);
    }
    write_start_tag(output, name, options, top_level, false);
    let member = if options.version == 2 {
        "polygonMember"
    } else {
        "surfaceMember"
    };
    for polygon in multi_polygon.polygons() {
        output.push('<');
        output.push_str(&options.prefix);
        output.push_str(member);
        output.push('>');
        write_polygon(output, &polygon, options, false)?;
        output.push_str("</");
        output.push_str(&options.prefix);
        output.push_str(member);
        output.push('>');
    }
    write_close_tag(output, name, options);
    Ok(())
}

fn write_geometry_collection(
    output: &mut String,
    collection: &impl GeometryCollectionTrait<T = f64>,
    options: &GmlWriteOptions,
    top_level: bool,
) -> Result<()> {
    if collection.num_geometries() == 0 {
        return write_empty_tag(output, "MultiGeometry", options, top_level);
    }
    write_start_tag(output, "MultiGeometry", options, top_level, false);
    for geometry in collection.geometries() {
        output.push('<');
        output.push_str(&options.prefix);
        output.push_str("geometryMember>");
        write_geometry_element(output, &geometry, options, false)?;
        output.push_str("</");
        output.push_str(&options.prefix);
        output.push_str("geometryMember>");
    }
    write_close_tag(output, "MultiGeometry", options);
    Ok(())
}

fn write_gml2_coordinate_element<C: CoordTrait<T = f64>>(
    output: &mut String,
    coordinates: impl Iterator<Item = C>,
    options: &GmlWriteOptions,
) -> Result<()> {
    output.push('<');
    output.push_str(&options.prefix);
    output.push_str("coordinates>");
    for (index, coordinate) in coordinates.enumerate() {
        if index > 0 {
            output.push(' ');
        }
        write_coordinate_values(output, &coordinate, options, ',')?;
    }
    output.push_str("</");
    output.push_str(&options.prefix);
    output.push_str("coordinates>");
    Ok(())
}

fn write_gml3_pos<C: CoordTrait<T = f64>>(
    output: &mut String,
    coordinate: C,
    options: &GmlWriteOptions,
) -> Result<()> {
    write_gml3_pos_element(output, "pos", std::iter::once(coordinate), options)
}

fn write_gml3_pos_list<C: CoordTrait<T = f64>>(
    output: &mut String,
    coordinates: impl Iterator<Item = C>,
    options: &GmlWriteOptions,
) -> Result<()> {
    write_gml3_pos_element(output, "posList", coordinates, options)
}

fn write_gml3_pos_element<C: CoordTrait<T = f64>>(
    output: &mut String,
    element: &str,
    mut coordinates: impl Iterator<Item = C>,
    options: &GmlWriteOptions,
) -> Result<()> {
    let Some(first) = coordinates.next() else {
        return Ok(());
    };
    ensure_gml_output_dimension(&first)?;
    let dimension = coordinate_dimension(&first);
    output.push('<');
    output.push_str(&options.prefix);
    output.push_str(element);
    if !options.omit_dimension() {
        output.push_str(&format!(" srsDimension=\"{dimension}\""));
    }
    output.push('>');
    write_coordinate_values(output, &first, options, ' ')?;
    for coordinate in coordinates {
        ensure_gml_output_dimension(&coordinate)?;
        if coordinate_dimension(&coordinate) != dimension {
            return Err(DataFusionError::Execution(
                "ST_AsGML: coordinate dimensions differ".to_string(),
            ));
        }
        output.push(' ');
        write_coordinate_values(output, &coordinate, options, ' ')?;
    }
    output.push_str("</");
    output.push_str(&options.prefix);
    output.push_str(element);
    output.push('>');
    Ok(())
}

fn write_coordinate_values(
    output: &mut String,
    coordinate: &impl CoordTrait<T = f64>,
    options: &GmlWriteOptions,
    separator: char,
) -> Result<()> {
    ensure_gml_output_dimension(coordinate)?;
    let (x, y) = if options.reverse_axis() {
        (coordinate.y(), coordinate.x())
    } else {
        (coordinate.x(), coordinate.y())
    };
    output.push_str(&format_ordinate(x, options.precision));
    output.push(separator);
    output.push_str(&format_ordinate(y, options.precision));
    if let Some(value) = coordinate.nth(2) {
        output.push(separator);
        output.push_str(&format_ordinate(value, options.precision));
    }
    if let Some(value) = coordinate.nth(3) {
        output.push(separator);
        output.push_str(&format_ordinate(value, options.precision));
    }
    Ok(())
}

fn ensure_gml_output_dimension(coordinate: &impl CoordTrait<T = f64>) -> Result<()> {
    if coordinate.dim() == geo_traits::Dimensions::Xym {
        return Err(DataFusionError::NotImplemented(
            "ST_AsGML: GML cannot distinguish an XYM coordinate from XYZ without additional CRS axes"
                .to_string(),
        ));
    }
    Ok(())
}

fn coordinate_dimension(coordinate: &impl CoordTrait<T = f64>) -> usize {
    match coordinate.dim() {
        geo_traits::Dimensions::Xy => 2,
        geo_traits::Dimensions::Xyz | geo_traits::Dimensions::Xym => 3,
        geo_traits::Dimensions::Xyzm => 4,
        geo_traits::Dimensions::Unknown(_) => 0,
    }
}

fn write_start_tag(
    output: &mut String,
    name: &str,
    options: &GmlWriteOptions,
    top_level: bool,
    empty: bool,
) {
    output.push('<');
    output.push_str(&options.prefix);
    output.push_str(name);
    if top_level {
        if let Some(srs) = srs_name(options) {
            output.push_str(" srsName=\"");
            output.push_str(&escape_attribute(&srs));
            output.push('"');
        }
        if options.version == 3
            && let Some(id) = &options.id
        {
            output.push(' ');
            output.push_str(&options.prefix);
            output.push_str("id=\"");
            output.push_str(&escape_attribute(id));
            output.push('"');
        }
    }
    output.push(if empty { '/' } else { '>' });
    if empty {
        output.push('>');
    }
}

fn write_empty_tag(
    output: &mut String,
    name: &str,
    options: &GmlWriteOptions,
    top_level: bool,
) -> Result<()> {
    write_start_tag(output, name, options, top_level, true);
    Ok(())
}

fn write_close_tag(output: &mut String, name: &str, options: &GmlWriteOptions) {
    output.push_str("</");
    output.push_str(&options.prefix);
    output.push_str(name);
    output.push('>');
}

fn srs_name(options: &GmlWriteOptions) -> Option<String> {
    if options.srid == 0 {
        None
    } else if options.long_crs() {
        Some(format!("urn:ogc:def:crs:EPSG::{}", options.srid))
    } else {
        Some(format!("EPSG:{}", options.srid))
    }
}

fn escape_attribute(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('\'', "&apos;")
}

fn geometry_is_empty(geometry: &impl GeometryTrait<T = f64>) -> Result<bool> {
    match geometry.as_type() {
        NativeGeometryType::Point(point) => Ok(point.coord().is_none()),
        NativeGeometryType::LineString(line) => Ok(line.num_coords() == 0),
        NativeGeometryType::Polygon(polygon) => {
            Ok(polygon.exterior().is_none_or(|ring| ring.num_coords() == 0))
        }
        NativeGeometryType::MultiPoint(multi_point) => Ok(multi_point.num_points() == 0
            || multi_point.points().all(|point| point.coord().is_none())),
        NativeGeometryType::MultiLineString(multi_line) => Ok(multi_line.num_line_strings() == 0
            || multi_line.line_strings().all(|line| line.num_coords() == 0)),
        NativeGeometryType::MultiPolygon(multi_polygon) => Ok(multi_polygon.num_polygons() == 0
            || multi_polygon
                .polygons()
                .all(|polygon| polygon.exterior().is_none())),
        NativeGeometryType::GeometryCollection(collection) => {
            if collection.num_geometries() == 0 {
                return Ok(true);
            }
            for child in collection.geometries() {
                if !geometry_is_empty(&child)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        NativeGeometryType::Rect(_)
        | NativeGeometryType::Triangle(_)
        | NativeGeometryType::Line(_) => Ok(false),
    }
}

#[derive(Debug, Clone, Copy)]
struct Envelope {
    dimension: usize,
    min: [f64; 4],
    max: [f64; 4],
}

fn write_envelope(
    output: &mut String,
    geometry: &impl GeometryTrait<T = f64>,
    options: &GmlWriteOptions,
) -> Result<()> {
    let Some(envelope) = envelope_of(geometry)? else {
        return Ok(());
    };
    let srs = srs_name(options);
    if options.version == 2 {
        output.push('<');
        output.push_str(&options.prefix);
        output.push_str("Box");
        if let Some(srs) = &srs {
            output.push_str(" srsName=\"");
            output.push_str(&escape_attribute(srs));
            output.push('"');
        }
        output.push('>');
        output.push('<');
        output.push_str(&options.prefix);
        output.push_str("coordinates>");
        write_envelope_coordinate(output, envelope.min, envelope.dimension, options);
        output.push(' ');
        write_envelope_coordinate(output, envelope.max, envelope.dimension, options);
        output.push_str("</");
        output.push_str(&options.prefix);
        output.push_str("coordinates></");
        output.push_str(&options.prefix);
        output.push_str("Box>");
    } else {
        output.push('<');
        output.push_str(&options.prefix);
        output.push_str("Envelope");
        if let Some(srs) = &srs {
            output.push_str(" srsName=\"");
            output.push_str(&escape_attribute(srs));
            output.push('"');
        }
        if !options.omit_dimension() {
            output.push_str(&format!(" srsDimension=\"{}\"", envelope.dimension));
        }
        output.push('>');
        output.push('<');
        output.push_str(&options.prefix);
        output.push_str("lowerCorner>");
        write_envelope_coordinate(output, envelope.min, envelope.dimension, options);
        output.push_str("</");
        output.push_str(&options.prefix);
        output.push_str("lowerCorner><");
        output.push_str(&options.prefix);
        output.push_str("upperCorner>");
        write_envelope_coordinate(output, envelope.max, envelope.dimension, options);
        output.push_str("</");
        output.push_str(&options.prefix);
        output.push_str("upperCorner></");
        output.push_str(&options.prefix);
        output.push_str("Envelope>");
    }
    Ok(())
}

fn write_envelope_coordinate(
    output: &mut String,
    values: [f64; 4],
    dimension: usize,
    options: &GmlWriteOptions,
) {
    let values = if options.reverse_axis() {
        [values[1], values[0], values[2], values[3]]
    } else {
        values
    };
    for (index, value) in values.iter().take(dimension).enumerate() {
        if index > 0 {
            output.push(if options.version == 2 { ',' } else { ' ' });
        }
        output.push_str(&format_ordinate(*value, options.precision));
    }
}

fn envelope_of(geometry: &impl GeometryTrait<T = f64>) -> Result<Option<Envelope>> {
    let mut envelope: Option<Envelope> = None;
    walk_coords(geometry, &mut |dimensions, values| {
        let dimension = match dimensions {
            geo_traits::Dimensions::Xy => 2,
            geo_traits::Dimensions::Xyz | geo_traits::Dimensions::Xym => 3,
            geo_traits::Dimensions::Xyzm => 4,
            geo_traits::Dimensions::Unknown(_) => {
                return Err(DataFusionError::Execution(
                    "ST_AsGML: unknown coordinate dimension".to_string(),
                ));
            }
        };
        if dimension == 3 && dimensions == geo_traits::Dimensions::Xym {
            return Err(DataFusionError::NotImplemented(
                "ST_AsGML: GML cannot distinguish an XYM coordinate from XYZ without additional CRS axes"
                    .to_string(),
            ));
        }
        match &mut envelope {
            Some(existing) => {
                if existing.dimension != dimension {
                    return Err(DataFusionError::Execution(
                        "ST_AsGML: envelope coordinates use mixed dimensions".to_string(),
                    ));
                }
                for (index, value) in values.iter().take(dimension).enumerate() {
                    existing.min[index] = existing.min[index].min(*value);
                    existing.max[index] = existing.max[index].max(*value);
                }
            }
            None => {
                envelope = Some(Envelope {
                    dimension,
                    min: values,
                    max: values,
                });
            }
        }
        Ok(())
    })?;
    Ok(envelope)
}

fn walk_coords(
    geometry: &impl GeometryTrait<T = f64>,
    callback: &mut impl FnMut(geo_traits::Dimensions, [f64; 4]) -> Result<()>,
) -> Result<()> {
    match geometry.as_type() {
        NativeGeometryType::Point(point) => {
            if let Some(coord) = point.coord() {
                callback(coord.dim(), coord_values(&coord))?;
            }
        }
        NativeGeometryType::LineString(line) => {
            for coord in line.coords() {
                callback(coord.dim(), coord_values(&coord))?;
            }
        }
        NativeGeometryType::Polygon(polygon) => {
            if let Some(ring) = polygon.exterior() {
                for coord in ring.coords() {
                    callback(coord.dim(), coord_values(&coord))?;
                }
            }
            for ring in polygon.interiors() {
                for coord in ring.coords() {
                    callback(coord.dim(), coord_values(&coord))?;
                }
            }
        }
        NativeGeometryType::MultiPoint(multi_point) => {
            for point in multi_point.points() {
                if let Some(coord) = point.coord() {
                    callback(coord.dim(), coord_values(&coord))?;
                }
            }
        }
        NativeGeometryType::MultiLineString(multi_line) => {
            for line in multi_line.line_strings() {
                for coord in line.coords() {
                    callback(coord.dim(), coord_values(&coord))?;
                }
            }
        }
        NativeGeometryType::MultiPolygon(multi_polygon) => {
            for polygon in multi_polygon.polygons() {
                if let Some(ring) = polygon.exterior() {
                    for coord in ring.coords() {
                        callback(coord.dim(), coord_values(&coord))?;
                    }
                }
                for ring in polygon.interiors() {
                    for coord in ring.coords() {
                        callback(coord.dim(), coord_values(&coord))?;
                    }
                }
            }
        }
        NativeGeometryType::GeometryCollection(collection) => {
            for child in collection.geometries() {
                walk_coords(&child, callback)?;
            }
        }
        NativeGeometryType::Rect(_)
        | NativeGeometryType::Triangle(_)
        | NativeGeometryType::Line(_) => {
            return Err(unsupported_gml("geometry type", "ST_AsGML"));
        }
    }
    Ok(())
}

fn coord_values(coord: &impl CoordTrait<T = f64>) -> [f64; 4] {
    [
        coord.x(),
        coord.y(),
        coord.nth(2).unwrap_or(f64::NAN),
        coord.nth(3).unwrap_or(f64::NAN),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(input: &str) -> Wkt<f64> {
        parse_gml(input, "ST_GeomFromGML").unwrap().geometry
    }

    #[test]
    fn parses_gml3_point_with_epsg() {
        let parsed = parse_gml(
            r#"<gml:Point xmlns:gml="http://www.opengis.net/gml/3.2" srsName="EPSG:4326"><gml:pos>30 10</gml:pos></gml:Point>"#,
            "ST_GeomFromGML",
        )
        .unwrap();
        assert_eq!(parsed.srid, 4326);
        assert_eq!(parsed.geometry.to_string(), "POINT(30 10)");
    }

    #[test]
    fn reverses_authority_axis_order_for_urn() {
        let parsed = parse_gml(
            r#"<gml:Point xmlns:gml="http://www.opengis.net/gml/3.2" srsName="urn:ogc:def:crs:EPSG::4326"><gml:pos>10 20</gml:pos></gml:Point>"#,
            "ST_GeomFromGML",
        )
        .unwrap();
        assert_eq!(parsed.srid, 4326);
        assert_eq!(parsed.geometry.to_string(), "POINT(20 10)");
    }

    #[test]
    fn does_not_swap_geocentric_urn_but_swaps_geographic_3d() {
        let geocentric = parse_gml(
            r#"<gml:Point xmlns:gml="http://www.opengis.net/gml/3.2" srsName="urn:ogc:def:crs:EPSG::4978"><gml:pos srsDimension="3">1 2 3</gml:pos></gml:Point>"#,
            "ST_GeomFromGML",
        )
        .unwrap();
        assert_eq!(geocentric.srid, 4978);
        assert_eq!(geocentric.geometry.to_string(), "POINT Z(1 2 3)");

        let geographic = parse_gml(
            r#"<gml:Point xmlns:gml="http://www.opengis.net/gml/3.2" srsName="urn:ogc:def:crs:EPSG::4979"><gml:pos srsDimension="3">1 2 3</gml:pos></gml:Point>"#,
            "ST_GeomFromGML",
        )
        .unwrap();
        assert_eq!(geographic.srid, 4979);
        assert_eq!(geographic.geometry.to_string(), "POINT Z(2 1 3)");
    }

    #[test]
    fn parses_gml2_polygon_with_hole() {
        let geometry = parse(
            r#"<gml:Polygon xmlns:gml="http://www.opengis.net/gml">
                <gml:outerBoundaryIs><gml:LinearRing><gml:coordinates>0,0 0,10 10,10 10,0 0,0</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs>
                <gml:innerBoundaryIs><gml:LinearRing><gml:coordinates>2,2 2,4 4,4 4,2 2,2</gml:coordinates></gml:LinearRing></gml:innerBoundaryIs>
            </gml:Polygon>"#,
        );
        assert_eq!(
            geometry.to_string(),
            "POLYGON((0 0,0 10,10 10,10 0,0 0),(2 2,2 4,4 4,4 2,2 2))"
        );
    }

    #[test]
    fn parses_mixed_geometry_collection() {
        let geometry = parse(
            r#"<gml:MultiGeometry xmlns:gml="http://www.opengis.net/gml/3.2">
                <gml:geometryMember><gml:Point><gml:pos>1 2</gml:pos></gml:Point></gml:geometryMember>
                <gml:geometryMember><gml:LineString><gml:posList>0 0 1 1</gml:posList></gml:LineString></gml:geometryMember>
            </gml:MultiGeometry>"#,
        );
        assert_eq!(
            geometry.to_string(),
            "GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(0 0,1 1))"
        );
    }

    #[test]
    fn parses_four_dimensional_pos() {
        let geometry = parse(
            r#"<gml:Point xmlns:gml="http://www.opengis.net/gml/3.2"><gml:pos srsDimension="4">1 2 3 4</gml:pos></gml:Point>"#,
        );
        assert_eq!(geometry.to_string(), "POINT ZM(1 2 3 4)");
    }

    #[test]
    fn parses_gml_empty_point() {
        let geometry = parse(r#"<gml:Point xmlns:gml="http://www.opengis.net/gml/3.2"/>"#);
        assert_eq!(geometry.to_string(), "POINT EMPTY");
    }

    fn writer_options(version: u8, options: i32) -> GmlWriteOptions {
        GmlWriteOptions {
            version,
            precision: 15,
            options,
            prefix: "gml:".to_string(),
            id: None,
            srid: 0,
        }
    }

    #[test]
    fn writes_gml2_and_gml3_as_postgis() {
        let parsed = parse(
            r#"<gml:Point xmlns:gml="http://www.opengis.net/gml/3.2"><gml:pos>1.234567890123456 2.345678901234567</gml:pos></gml:Point>"#,
        );
        let gml2 = write_gml(&parsed, &writer_options(2, 0)).unwrap().unwrap();
        assert_eq!(
            gml2,
            "<gml:Point><gml:coordinates>1.234567890123456,2.345678901234567</gml:coordinates></gml:Point>"
        );
        let gml3 = write_gml(&parsed, &writer_options(3, 0)).unwrap().unwrap();
        assert_eq!(
            gml3,
            "<gml:Point><gml:pos srsDimension=\"2\">1.234567890123456 2.345678901234567</gml:pos></gml:Point>"
        );
    }

    #[test]
    fn writes_line_curve_with_option4() {
        let parsed = parse(
            r#"<gml:LineString xmlns:gml="http://www.opengis.net/gml/3.2"><gml:posList>1 2 3 4</gml:posList></gml:LineString>"#,
        );
        let curve = write_gml(&parsed, &writer_options(3, 0)).unwrap().unwrap();
        assert!(curve.contains("<gml:Curve"));
        assert!(curve.contains("<gml:LineStringSegment>"));
        let line = write_gml(&parsed, &writer_options(3, 4)).unwrap().unwrap();
        assert!(line.starts_with("<gml:LineString"));
        assert!(!line.contains("<gml:Curve"));
    }

    #[test]
    fn empty_geometry_writes_null() {
        let parsed = parse(r#"<gml:Point xmlns:gml="http://www.opengis.net/gml/3.2"/>"#);
        assert!(write_gml(&parsed, &writer_options(2, 0)).unwrap().is_none());
        assert!(write_gml(&parsed, &writer_options(3, 0)).unwrap().is_none());
    }
}

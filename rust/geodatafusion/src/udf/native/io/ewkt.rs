use std::borrow::Cow;
use std::sync::{Arc, OnceLock};

use arrow_array::builder::{GenericStringBuilder, StringBuilder, StringViewBuilder};
use arrow_array::{
    Array, ArrayRef, Int32Array, Int64Array, LargeStringArray, StringArray, StringViewArray,
};
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
use geoarrow_schema::{CoordType, GeometryType, Metadata, WktType};

use super::common::{present_ewkb_srids, srid_from_metadata, validate_embedded_srids};
use super::coordinate::format_ordinate;
use crate::data_types::any_geometry_type;
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::{
    expand_rect_to_geometry, field_metadata, geometry_array_from_arrow, metadata_for_srid,
    normalize_srid,
};

const DEFAULT_PRECISION: i32 = 15;

fn as_ewkt_signature() -> Signature {
    let geometry_types = any_geometry_type();
    let mut signatures = vec![TypeSignature::Uniform(1, geometry_types.clone())];
    for geometry_type in geometry_types {
        signatures.push(TypeSignature::Exact(vec![
            geometry_type.clone(),
            DataType::Int32,
        ]));
        signatures.push(TypeSignature::Exact(vec![geometry_type, DataType::Int64]));
    }
    Signature::one_of(signatures, Volatility::Immutable)
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct AsEWKT {
    signature: Signature,
}

impl AsEWKT {
    pub fn new() -> Self {
        Self {
            signature: as_ewkt_signature(),
        }
    }
}

impl Default for AsEWKT {
    fn default() -> Self {
        Self::new()
    }
}

static AS_EWKT_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for AsEWKT {
    fn name(&self) -> &str {
        "st_asewkt"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<Field>> {
        let input_field = &args.arg_fields[0];
        let metadata = field_metadata(input_field)?;
        srid_from_metadata(&metadata, self.name())?;
        let wkt_type = WktType::new(metadata);
        Ok(Field::new(
            input_field.name(),
            DataType::Utf8,
            input_field.is_nullable(),
        )
        .with_extension_type(wkt_type)
        .into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let metadata = Metadata::try_from(args.return_field.as_ref())?;
        let srid = srid_from_metadata(&metadata, self.name())?;
        if let Some(embedded_srids) = present_ewkb_srids(&arrays[0], &args.arg_fields[0])? {
            validate_embedded_srids(self.name(), srid, &embedded_srids)?;
        }
        let geo_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
        let precision = PrecisionColumn::from_array(arrays.get(1))?;
        let expanded = expand_rect_to_geometry(geo_array.as_ref())?;
        let geo_array: &dyn GeoArrowArray = expanded
            .as_ref()
            .map_or(geo_array.as_ref(), |expanded| expanded);
        let result = downcast_geoarrow_array!(geo_array, write_ewkt_array, &precision, srid)?;
        Ok(ColumnarValue::Array(Arc::new(result)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(AS_EWKT_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the Extended Well-Known Text (EWKT) representation with SRID metadata.",
                "ST_AsEWKT(geometry[, maxdecimaldigits])",
            )
            .with_argument("g1", "geometry")
            .with_argument("maxdecimaldigits", "optional maximum decimal digits")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct GeomFromEWKT {
    coord_type: CoordType,
}

impl GeomFromEWKT {
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let field = &args.arg_fields[0];
        let (array, srids) = prepare_ewkt_input(&arrays[0], field.as_ref())?;
        let metadata = Arc::new(Metadata::try_from(args.return_field.as_ref())?);
        let expected_srid = srid_from_metadata(&metadata, self.name())?;
        let to_type = GeometryType::new(metadata).with_coord_type(self.coord_type);
        let geom_arr = super::wkt::decode_wkt_array(
            array.as_ref(),
            to_type,
            None,
            Some((self.name(), expected_srid, &srids)),
        )?;

        Ok(ColumnarValue::Array(geom_arr))
    }
}

impl Default for GeomFromEWKT {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static GEOM_FROM_EWKT_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for GeomFromEWKT {
    fn name(&self) -> &str {
        "st_geomfromewkt"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: std::sync::OnceLock<Signature> = std::sync::OnceLock::new();
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

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<Field>> {
        let input_field = &args.arg_fields[0];
        let metadata = match args.scalar_arguments.first().copied().flatten() {
            Some(ScalarValue::Utf8(Some(value)))
            | Some(ScalarValue::LargeUtf8(Some(value)))
            | Some(ScalarValue::Utf8View(Some(value))) => {
                metadata_for_srid(parse_ewkt_srid(value)?)
            }
            Some(ScalarValue::Utf8(None))
            | Some(ScalarValue::LargeUtf8(None))
            | Some(ScalarValue::Utf8View(None))
            | Some(ScalarValue::Null) => metadata_for_srid(0),
            _ => {
                let metadata = field_metadata(input_field)?;
                srid_from_metadata(&metadata, self.name())?;
                metadata
            }
        };
        let geom_type = GeometryType::new(metadata).with_coord_type(self.coord_type);
        Ok(geom_type
            .to_field(input_field.name(), input_field.is_nullable())
            .into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(self.invoke_with_args(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(GEOM_FROM_EWKT_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Constructs a geometry from Extended Well-Known Text (EWKT).",
                "ST_GeomFromEWKT(ewkt)",
            )
            .with_argument("ewkt", "extended well-known text")
            .build()
        }))
    }
}

fn parse_ewkt_srid(input: &str) -> Result<i32> {
    let input = input.trim_start();
    let Some(prefix) = input.get(..5) else {
        return Ok(0);
    };
    if !prefix.eq_ignore_ascii_case("SRID=") {
        return Ok(0);
    }

    let Some(rest) = input.get(5..) else {
        return Ok(0);
    };
    let (srid, _) = rest.split_once(';').ok_or_else(|| {
        DataFusionError::Execution("EWKT SRID prefix is missing a semicolon".to_string())
    })?;
    let srid = srid.trim();
    if !srid
        .as_bytes()
        .first()
        .is_some_and(|byte| byte.is_ascii_digit() || *byte == b'-')
    {
        return Err(DataFusionError::Execution(format!(
            "invalid EWKT SRID {srid:?}"
        )));
    }
    let srid = srid
        .parse::<i64>()
        .map_err(|_| DataFusionError::Execution(format!("invalid EWKT SRID {srid:?}")))?;
    Ok(normalize_srid(srid)?)
}

/// Split the optional `SRID=<n>;` prefix from an EWKT literal.
fn strip_ewkt_srid(value: &str) -> &str {
    let trimmed = value.trim_start();
    let Some(prefix) = trimmed.get(..5) else {
        return value;
    };
    if !prefix.eq_ignore_ascii_case("SRID=") {
        return value;
    }
    trimmed
        .get(5..)
        .and_then(|rest| rest.split_once(';'))
        .map_or(value, |(_, geometry)| geometry)
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum EwktDimension {
    Xyz,
    Xym,
    Xyzm,
}

impl EwktDimension {
    fn marker(self) -> &'static str {
        match self {
            Self::Xyz => "Z",
            Self::Xym => "M",
            Self::Xyzm => "ZM",
        }
    }

    fn ordinates(self) -> usize {
        match self {
            Self::Xyz | Self::Xym => 3,
            Self::Xyzm => 4,
        }
    }
}

enum EwktToken<'a> {
    Word(&'a str),
    Punct,
}

const EWKT_TYPE_KEYWORDS: [&str; 7] = [
    "POINT",
    "LINESTRING",
    "POLYGON",
    "MULTIPOINT",
    "MULTILINESTRING",
    "MULTIPOLYGON",
    "GEOMETRYCOLLECTION",
];

fn ewkt_tokens(input: &str) -> Vec<(usize, usize, EwktToken<'_>)> {
    let bytes = input.as_bytes();
    let mut tokens = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte.is_ascii_whitespace() {
            index += 1;
            continue;
        }
        if matches!(byte, b'(' | b')' | b',') {
            tokens.push((index, index + 1, EwktToken::Punct));
            index += 1;
            continue;
        }
        let start = index;
        while index < bytes.len()
            && !bytes[index].is_ascii_whitespace()
            && !matches!(bytes[index], b'(' | b')' | b',')
        {
            index += 1;
        }
        tokens.push((start, index, EwktToken::Word(&input[start..index])));
    }
    tokens
}

/// Classify a geometry type keyword, including any attached `Z`, `M`, or `ZM`.
fn ewkt_type_keyword(word: &str) -> Option<Option<EwktDimension>> {
    for (suffix, dimension) in [
        ("ZM", EwktDimension::Xyzm),
        ("Z", EwktDimension::Xyz),
        ("M", EwktDimension::Xym),
    ] {
        let Some(base) = word.get(..word.len().saturating_sub(suffix.len())) else {
            continue;
        };
        let Some(tail) = word.get(word.len().saturating_sub(suffix.len())..) else {
            continue;
        };
        if !base.is_empty()
            && tail.eq_ignore_ascii_case(suffix)
            && EWKT_TYPE_KEYWORDS
                .iter()
                .any(|keyword| base.eq_ignore_ascii_case(keyword))
        {
            return Some(Some(dimension));
        }
    }
    EWKT_TYPE_KEYWORDS
        .iter()
        .any(|keyword| word.eq_ignore_ascii_case(keyword))
        .then_some(None)
}

fn ewkt_dimension_marker(word: &str) -> Option<EwktDimension> {
    if word.eq_ignore_ascii_case("ZM") {
        Some(EwktDimension::Xyzm)
    } else if word.eq_ignore_ascii_case("Z") {
        Some(EwktDimension::Xyz)
    } else if word.eq_ignore_ascii_case("M") {
        Some(EwktDimension::Xym)
    } else {
        None
    }
}

fn merge_dimension(explicit: &mut Option<EwktDimension>, dimension: EwktDimension) -> Result<()> {
    match explicit {
        Some(existing) if *existing != dimension => Err(DataFusionError::Execution(
            "EWKT geometry mixes coordinate dimensions".to_string(),
        )),
        Some(_) => Ok(()),
        None => {
            *explicit = Some(dimension);
            Ok(())
        }
    }
}

fn flush_run(runs: &mut Vec<usize>, current: &mut usize) {
    if *current > 0 {
        runs.push(*current);
        *current = 0;
    }
}

/// Insert explicit dimension markers where the coordinate count implies them.
///
/// PostGIS prints `POINT(1 2 3)` for XYZ and `POINT(1 2 3 4)` for XYZM but
/// accepts them as their implied dimensions. The `wkt` parser requires the
/// markers, so EWKT input is normalized before parsing.
fn normalize_ewkt_dimensions(input: &str) -> Result<Cow<'_, str>> {
    let tokens = ewkt_tokens(input);
    let mut explicit = None;
    let mut runs = Vec::new();
    let mut current_run = 0;
    let mut after_type_keyword = false;

    for (_, _, token) in &tokens {
        match token {
            EwktToken::Word(word) => {
                if let Some(attached) = ewkt_type_keyword(word) {
                    if let Some(dimension) = attached {
                        merge_dimension(&mut explicit, dimension)?;
                    }
                    after_type_keyword = true;
                    continue;
                }
                if after_type_keyword && let Some(dimension) = ewkt_dimension_marker(word) {
                    merge_dimension(&mut explicit, dimension)?;
                    after_type_keyword = false;
                    continue;
                }
                after_type_keyword = false;
                if word.parse::<f64>().is_ok() {
                    current_run += 1;
                } else {
                    flush_run(&mut runs, &mut current_run);
                }
            }
            EwktToken::Punct => {
                after_type_keyword = false;
                flush_run(&mut runs, &mut current_run);
            }
        }
    }
    flush_run(&mut runs, &mut current_run);

    let dimension = match explicit {
        Some(dimension) => {
            let expected = dimension.ordinates();
            if runs.iter().any(|count| *count != expected) {
                return Err(DataFusionError::Execution(format!(
                    "EWKT dimension mismatch: expected {expected} ordinates per coordinate"
                )));
            }
            dimension
        }
        None => {
            let Some(first) = runs.first().copied() else {
                return Ok(Cow::Borrowed(input));
            };
            if runs.iter().any(|count| *count != first) {
                return Err(DataFusionError::Execution(
                    "EWKT geometry mixes coordinate dimensions".to_string(),
                ));
            }
            match first {
                2 => return Ok(Cow::Borrowed(input)),
                3 => EwktDimension::Xyz,
                4 => EwktDimension::Xyzm,
                count => {
                    return Err(DataFusionError::Execution(format!(
                        "unsupported EWKT coordinate with {count} ordinates"
                    )));
                }
            }
        }
    };
    let marker = dimension.marker();

    let mut output = String::with_capacity(input.len() + 8);
    let mut cursor = 0;
    for (index, (_, end, token)) in tokens.iter().enumerate() {
        let EwktToken::Word(word) = token else {
            continue;
        };
        if ewkt_type_keyword(word).is_none_or(|attached| attached.is_some()) {
            continue;
        }
        let has_explicit_marker = tokens.get(index + 1).is_some_and(|(_, _, next)| {
            matches!(next, EwktToken::Word(word) if ewkt_dimension_marker(word).is_some())
        });
        if has_explicit_marker {
            continue;
        }
        output.push_str(&input[cursor..*end]);
        output.push(' ');
        output.push_str(marker);
        cursor = *end;
    }
    if output.is_empty() {
        Ok(Cow::Borrowed(input))
    } else {
        output.push_str(&input[cursor..]);
        Ok(Cow::Owned(output))
    }
}

/// Read per-row SRIDs and return the WKT array with EWKT prefixes removed.
fn prepare_ewkt_input(array: &ArrayRef, field: &Field) -> Result<(ArrayRef, Vec<Option<i32>>)> {
    match field.data_type() {
        DataType::Utf8 => {
            let values = array.as_any().downcast_ref::<StringArray>().unwrap();
            prepare_ewkt_values(values.iter(), array, |prepared| {
                let mut builder = StringBuilder::with_capacity(
                    prepared.len(),
                    prepared.iter().flatten().map(|value| value.len()).sum(),
                );
                for value in prepared {
                    match value {
                        Some(value) => builder.append_value(value.as_ref()),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            })
        }
        DataType::LargeUtf8 => {
            let values = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            prepare_ewkt_values(values.iter(), array, |prepared| {
                let mut builder = GenericStringBuilder::<i64>::with_capacity(
                    prepared.len(),
                    prepared.iter().flatten().map(|value| value.len()).sum(),
                );
                for value in prepared {
                    match value {
                        Some(value) => builder.append_value(value.as_ref()),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            })
        }
        DataType::Utf8View => {
            let values = array.as_any().downcast_ref::<StringViewArray>().unwrap();
            prepare_ewkt_values(values.iter(), array, |prepared| {
                let mut builder = StringViewBuilder::with_capacity(prepared.len());
                for value in prepared {
                    match value {
                        Some(value) => builder.append_value(value.as_ref()),
                        None => builder.append_null(),
                    }
                }
                Arc::new(builder.finish())
            })
        }
        _ => unreachable!(),
    }
}

fn prepare_ewkt_values<'a>(
    values: impl Iterator<Item = Option<&'a str>>,
    unchanged: &ArrayRef,
    build: impl FnOnce(&[Option<Cow<'a, str>>]) -> ArrayRef,
) -> Result<(ArrayRef, Vec<Option<i32>>)> {
    let mut srids = Vec::new();
    let mut prepared = Vec::new();
    let mut changed = false;
    for value in values {
        match value {
            None => {
                srids.push(None);
                prepared.push(None);
            }
            Some(value) => {
                srids.push(Some(parse_ewkt_srid(value)?));
                let normalized = normalize_ewkt_dimensions(strip_ewkt_srid(value))?;
                changed |= match &normalized {
                    Cow::Borrowed(text) => *text != value,
                    Cow::Owned(_) => true,
                };
                prepared.push(Some(normalized));
            }
        }
    }
    if !changed {
        return Ok((Arc::clone(unchanged), srids));
    }
    Ok((build(&prepared), srids))
}

enum PrecisionColumn<'a> {
    Default,
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
}

impl<'a> PrecisionColumn<'a> {
    fn from_array(array: Option<&'a ArrayRef>) -> Result<Self> {
        let Some(array) = array else {
            return Ok(Self::Default);
        };
        match array.data_type() {
            DataType::Int32 => Ok(Self::Int32(array.as_any().downcast_ref().unwrap())),
            DataType::Int64 => Ok(Self::Int64(array.as_any().downcast_ref().unwrap())),
            other => Err(DataFusionError::Internal(format!(
                "unexpected ST_AsEWKT precision type {other}"
            ))),
        }
    }

    fn value(&self, row: usize) -> Option<i32> {
        match self {
            Self::Default => Some(DEFAULT_PRECISION),
            Self::Int32(array) if array.is_null(row) => None,
            Self::Int32(array) => Some(array.value(row)),
            Self::Int64(array) if array.is_null(row) => None,
            Self::Int64(array) => Some(i32::try_from(array.value(row)).unwrap_or(i32::MAX)),
        }
    }
}

fn write_ewkt_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    precision: &PrecisionColumn<'_>,
    srid: i32,
) -> GeoDataFusionResult<StringArray> {
    let mut builder = StringBuilder::with_capacity(array.len(), array.len() * 32);
    for (row, item) in array.iter().enumerate() {
        let Some(precision) = precision.value(row) else {
            builder.append_null();
            continue;
        };
        match item {
            None => builder.append_null(),
            Some(Err(err)) => return Err(err.into()),
            Some(Ok(geometry)) => {
                builder.append_value(write_ewkt(&geometry, srid, precision)?);
            }
        }
    }
    Ok(builder.finish())
}

fn write_ewkt(geometry: &impl GeometryTrait<T = f64>, srid: i32, precision: i32) -> Result<String> {
    let mut output = String::new();
    if srid != 0 {
        output.push_str(&format!("SRID={srid};"));
    }
    write_geometry_body(&mut output, geometry, precision)?;
    Ok(output)
}

fn write_geometry_body(
    output: &mut String,
    geometry: &impl GeometryTrait<T = f64>,
    precision: i32,
) -> Result<()> {
    match geometry.as_type() {
        NativeGeometryType::Point(point) => write_point(output, point, precision),
        NativeGeometryType::LineString(line_string) => {
            write_linestring(output, line_string, precision)
        }
        NativeGeometryType::Polygon(polygon) => write_polygon(output, polygon, precision),
        NativeGeometryType::MultiPoint(multi_point) => {
            write_multi_point(output, multi_point, precision)
        }
        NativeGeometryType::MultiLineString(multi_line_string) => {
            write_multi_linestring(output, multi_line_string, precision)
        }
        NativeGeometryType::MultiPolygon(multi_polygon) => {
            write_multi_polygon(output, multi_polygon, precision)
        }
        NativeGeometryType::GeometryCollection(collection) => {
            write_geometry_collection(output, collection, precision)
        }
        NativeGeometryType::Rect(_)
        | NativeGeometryType::Triangle(_)
        | NativeGeometryType::Line(_) => Err(DataFusionError::NotImplemented(
            "ST_AsEWKT only supports the seven GeoArrow geometry families".to_string(),
        )),
    }
}

fn write_type_prefix(output: &mut String, name: &str, dim: geo_traits::Dimensions) {
    output.push_str(name);
    if dim == geo_traits::Dimensions::Xym {
        output.push('M');
    }
}

fn write_point(
    output: &mut String,
    point: &impl PointTrait<T = f64>,
    precision: i32,
) -> Result<()> {
    write_type_prefix(output, "POINT", point.dim());
    let Some(coord) = point.coord() else {
        output.push_str(" EMPTY");
        return Ok(());
    };
    write_coord(output, &coord, precision);
    Ok(())
}

fn write_linestring(
    output: &mut String,
    line_string: &impl LineStringTrait<T = f64>,
    precision: i32,
) -> Result<()> {
    write_type_prefix(output, "LINESTRING", line_string.dim());
    if line_string.num_coords() == 0 {
        output.push_str(" EMPTY");
    } else {
        write_coord_sequence(output, line_string.coords(), precision);
    }
    Ok(())
}

fn write_polygon(
    output: &mut String,
    polygon: &impl PolygonTrait<T = f64>,
    precision: i32,
) -> Result<()> {
    write_type_prefix(output, "POLYGON", polygon.dim());
    let Some(exterior) = polygon.exterior() else {
        output.push_str(" EMPTY");
        return Ok(());
    };
    if exterior.num_coords() == 0 {
        output.push_str(" EMPTY");
        return Ok(());
    }
    output.push('(');
    write_coord_sequence(output, exterior.coords(), precision);
    for interior in polygon.interiors() {
        output.push(',');
        write_coord_sequence(output, interior.coords(), precision);
    }
    output.push(')');
    Ok(())
}

fn write_multi_point(
    output: &mut String,
    multi_point: &impl MultiPointTrait<T = f64>,
    precision: i32,
) -> Result<()> {
    write_type_prefix(output, "MULTIPOINT", multi_point.dim());
    if multi_point.num_points() == 0 {
        output.push_str(" EMPTY");
        return Ok(());
    }
    output.push('(');
    for (index, point) in multi_point.points().enumerate() {
        if index > 0 {
            output.push(',');
        }
        match point.coord() {
            Some(coord) => write_coord_values(output, &coord, precision),
            None => output.push_str("EMPTY"),
        }
    }
    output.push(')');
    Ok(())
}

fn write_multi_linestring(
    output: &mut String,
    multi_line_string: &impl MultiLineStringTrait<T = f64>,
    precision: i32,
) -> Result<()> {
    write_type_prefix(output, "MULTILINESTRING", multi_line_string.dim());
    if multi_line_string.num_line_strings() == 0 {
        output.push_str(" EMPTY");
        return Ok(());
    }
    output.push('(');
    for (index, line_string) in multi_line_string.line_strings().enumerate() {
        if index > 0 {
            output.push(',');
        }
        if line_string.num_coords() == 0 {
            output.push_str("EMPTY");
        } else {
            write_coord_sequence(output, line_string.coords(), precision);
        }
    }
    output.push(')');
    Ok(())
}

fn write_multi_polygon(
    output: &mut String,
    multi_polygon: &impl MultiPolygonTrait<T = f64>,
    precision: i32,
) -> Result<()> {
    write_type_prefix(output, "MULTIPOLYGON", multi_polygon.dim());
    if multi_polygon.num_polygons() == 0 {
        output.push_str(" EMPTY");
        return Ok(());
    }
    output.push('(');
    for (index, polygon) in multi_polygon.polygons().enumerate() {
        if index > 0 {
            output.push(',');
        }
        write_polygon_rings(output, &polygon, precision);
    }
    output.push(')');
    Ok(())
}

fn write_geometry_collection(
    output: &mut String,
    collection: &impl GeometryCollectionTrait<T = f64>,
    precision: i32,
) -> Result<()> {
    write_type_prefix(output, "GEOMETRYCOLLECTION", collection.dim());
    if collection.num_geometries() == 0 {
        output.push_str(" EMPTY");
        return Ok(());
    }
    output.push('(');
    for (index, geometry) in collection.geometries().enumerate() {
        if index > 0 {
            output.push(',');
        }
        write_geometry_body(output, &geometry, precision)?;
    }
    output.push(')');
    Ok(())
}

fn write_polygon_rings(output: &mut String, polygon: &impl PolygonTrait<T = f64>, precision: i32) {
    let Some(exterior) = polygon.exterior().filter(|ring| ring.num_coords() > 0) else {
        output.push_str("EMPTY");
        return;
    };
    output.push('(');
    write_coord_sequence(output, exterior.coords(), precision);
    for interior in polygon.interiors() {
        output.push(',');
        write_coord_sequence(output, interior.coords(), precision);
    }
    output.push(')');
}

fn write_coord_sequence(
    output: &mut String,
    coords: impl Iterator<Item = impl CoordTrait<T = f64>>,
    precision: i32,
) {
    output.push('(');
    for (index, coord) in coords.enumerate() {
        if index > 0 {
            output.push(',');
        }
        write_coord_values(output, &coord, precision);
    }
    output.push(')');
}

fn write_coord(output: &mut String, coord: &impl CoordTrait<T = f64>, precision: i32) {
    output.push('(');
    write_coord_values(output, coord, precision);
    output.push(')');
}

fn write_coord_values(output: &mut String, coord: &impl CoordTrait<T = f64>, precision: i32) {
    let dim = coord.dim();
    output.push_str(&format_ordinate(coord.x(), precision));
    output.push(' ');
    output.push_str(&format_ordinate(coord.y(), precision));
    match dim {
        geo_traits::Dimensions::Xyz | geo_traits::Dimensions::Xym => {
            if let Some(value) = coord.nth(2) {
                output.push(' ');
                output.push_str(&format_ordinate(value, precision));
            }
        }
        geo_traits::Dimensions::Xyzm => {
            if let Some(value) = coord.nth(2) {
                output.push(' ');
                output.push_str(&format_ordinate(value, precision));
            }
            if let Some(value) = coord.nth(3) {
                output.push(' ');
                output.push_str(&format_ordinate(value, precision));
            }
        }
        _ => {}
    }
}

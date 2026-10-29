use std::fmt;
use std::sync::{Arc, OnceLock};

use arrow_array::builder::StringBuilder;
use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, StringArray};
use arrow_schema::{DataType, Field};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, GeometryType as NativeGeometryType,
    LineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait,
};
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::cast::to_wkt;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::error::GeoArrowError;
use geoarrow_schema::{CoordType, GeoArrowType, GeometryType, Metadata, WktType};
use wkt::Wkt;
use wkt::types::{
    Dimension as WktDimension, GeometryCollection as WktGeometryCollection,
    MultiPoint as WktMultiPoint, Point as WktPoint,
};

use super::common::{
    GeometryKind, empty_geometry_array, push_decoded_geometry, typed_geometry_signature,
    typed_metadata_from_args,
};
use crate::data_types::any_single_geometry_type_input;
use crate::error::{GeoDataFusionError, GeoDataFusionResult};
use crate::udf::native::geometry::{StrictGeometryBuilder, rect_to_polygon, resolve_srid_argument};

/// Maximum `GeometryCollection` nesting accepted from WKT text.
///
/// This matches the `wkb` reader limit and the writer bound below: 128 levels
/// of collection nesting stay readable and deeper input is a controlled error.
pub(crate) const MAX_WKT_NESTING_DEPTH: usize = 128;

/// Reject WKT text whose `GeometryCollection` nesting exceeds
/// [`MAX_WKT_NESTING_DEPTH`].
///
/// `wkt` 0.14 parses collections recursively without a depth bound, and a
/// 2 MiB thread stack overflows below 500 levels, so every path that hands
/// text to that parser runs this iterative pre-check first.
pub(crate) fn ensure_wkt_text_depth(text: &str) -> GeoDataFusionResult<()> {
    if wkt_nesting_depth(text) > MAX_WKT_NESTING_DEPTH {
        return Err(GeoArrowError::Wkt(format!(
            "geometry collection nesting depth exceeds {MAX_WKT_NESTING_DEPTH}"
        ))
        .into());
    }
    Ok(())
}

/// Apply [`ensure_wkt_text_depth`] to a physical text column.
///
/// The scan follows the Arrow array, so slices, NULLs, and row order are
/// preserved exactly; non-text storage never parses through the `wkt` reader
/// and is accepted unchanged.
pub(crate) fn ensure_wkt_array_depth(array: &dyn Array) -> GeoDataFusionResult<()> {
    match array.data_type() {
        DataType::Utf8 => ensure_wkt_values_depth(array.as_string::<i32>().iter()),
        DataType::LargeUtf8 => ensure_wkt_values_depth(array.as_string::<i64>().iter()),
        DataType::Utf8View => ensure_wkt_values_depth(array.as_string_view().iter()),
        _ => Ok(()),
    }
}

fn ensure_wkt_values_depth<'a>(
    values: impl Iterator<Item = Option<&'a str>>,
) -> GeoDataFusionResult<()> {
    for value in values.flatten() {
        ensure_wkt_text_depth(value)?;
    }
    Ok(())
}

/// Deepest `GEOMETRYCOLLECTION` nesting opened by `text`.
fn wkt_nesting_depth(text: &str) -> usize {
    let bytes = text.as_bytes();
    let mut open_collections = Vec::new();
    let mut depth = 0usize;
    let mut max_depth = 0usize;
    for (index, byte) in bytes.iter().enumerate() {
        match byte {
            b'(' => {
                let collection = is_collection_open(bytes, index);
                depth += usize::from(collection);
                max_depth = max_depth.max(depth);
                open_collections.push(collection);
            }
            b')' => depth -= usize::from(open_collections.pop().unwrap_or(false)),
            _ => {}
        }
    }
    max_depth
}

/// Whether the `(` at `open_index` belongs to a `GEOMETRYCOLLECTION` word.
fn is_collection_open(bytes: &[u8], open_index: usize) -> bool {
    let mut cursor = open_index;
    let Some(word) = preceding_word(bytes, &mut cursor) else {
        return false;
    };
    if is_geometry_collection_word(word) {
        return true;
    }
    // `GEOMETRYCOLLECTION Z (` separates the dimension marker from the word.
    is_dimension_marker(word)
        && preceding_word(bytes, &mut cursor).is_some_and(is_geometry_collection_word)
}

fn preceding_word<'a>(bytes: &'a [u8], cursor: &mut usize) -> Option<&'a [u8]> {
    while *cursor > 0 && bytes[*cursor - 1].is_ascii_whitespace() {
        *cursor -= 1;
    }
    let end = *cursor;
    while *cursor > 0 && bytes[*cursor - 1].is_ascii_alphabetic() {
        *cursor -= 1;
    }
    (*cursor < end).then(|| &bytes[*cursor..end])
}

fn is_geometry_collection_word(word: &[u8]) -> bool {
    [
        "GEOMETRYCOLLECTION",
        "GEOMETRYCOLLECTIONZ",
        "GEOMETRYCOLLECTIONM",
        "GEOMETRYCOLLECTIONZM",
    ]
    .iter()
    .any(|keyword| word.eq_ignore_ascii_case(keyword.as_bytes()))
}

fn is_dimension_marker(word: &[u8]) -> bool {
    ["Z", "M", "ZM"]
        .iter()
        .any(|keyword| word.eq_ignore_ascii_case(keyword.as_bytes()))
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct AsText;

impl AsText {
    pub fn new() -> Self {
        Self {}
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
        let array = &ColumnarValue::values_to_arrays(&args.args)?[0];
        let field = &args.arg_fields[0];
        // WKT storage keeps serialized text byte-for-byte and never parses it,
        // so this branch stays on the raw conversion: deep columns read back
        // unchanged instead of hitting the parsable-input depth guard.
        let geo_array = from_arrow_array(&array, field.as_ref())?;
        match geo_array.data_type() {
            GeoArrowType::Wkt(_) | GeoArrowType::LargeWkt(_) | GeoArrowType::WktView(_) => {
                let wkt_arr = to_wkt::<i32>(geo_array.as_ref())?;
                Ok(ColumnarValue::Array(wkt_arr.into_array_ref()))
            }
            _ => {
                let geo_array_ref = geo_array.as_ref();
                let wkt_arr = downcast_geoarrow_array!(geo_array_ref, write_wkt_array)?;
                Ok(ColumnarValue::Array(Arc::new(wkt_arr)))
            }
        }
    }
}

fn write_wkt_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoDataFusionResult<StringArray> {
    let mut builder = StringBuilder::with_capacity(array.len(), array.len() * 32);
    for item in array.iter() {
        match item {
            None => builder.append_null(),
            Some(Ok(geometry)) => {
                let mut output = String::new();
                write_wkt_geometry(&mut output, &geometry)?;
                builder.append_value(output);
            }
            Some(Err(error)) => return Err(error.into()),
        }
    }
    Ok(builder.finish())
}

/// Write a geometry with the same layout as `wkt::to_wkt`.
///
/// The upstream writer assumes that multi-geometry members are non-empty.
/// GeoArrow permits empty points and polygons inside their multi geometries,
/// so those families are written here. Rect views use the shared polygon
/// conversion before formatting; other families use the upstream formatter.
fn write_wkt_geometry<W: fmt::Write>(
    output: &mut W,
    geometry: &impl GeometryTrait<T = f64>,
) -> GeoDataFusionResult<()> {
    if let NativeGeometryType::Rect(rect) = geometry.as_type() {
        return write_wkt_geometry_inner(output, &rect_to_polygon(rect)?, 0);
    }
    write_wkt_geometry_inner(output, geometry, 0)
}

fn write_wkt_geometry_inner<W: fmt::Write>(
    output: &mut W,
    geometry: &impl GeometryTrait<T = f64>,
    depth: usize,
) -> GeoDataFusionResult<()> {
    if depth > MAX_WKT_NESTING_DEPTH {
        return Err(GeoArrowError::Wkt(format!(
            "geometry collection nesting depth exceeds {MAX_WKT_NESTING_DEPTH}"
        ))
        .into());
    }
    match geometry.as_type() {
        NativeGeometryType::MultiPoint(multi_point) => {
            write_wkt_multi_point(output, multi_point).map_err(wkt_error)
        }
        NativeGeometryType::MultiPolygon(multi_polygon) => {
            write_wkt_multi_polygon(output, multi_polygon).map_err(wkt_error)
        }
        NativeGeometryType::GeometryCollection(collection) => {
            write_wkt_geometry_collection(output, collection, depth)
        }
        _ => wkt::to_wkt::write_geometry(output, geometry).map_err(wkt_error),
    }
}

fn wkt_error(error: wkt::error::Error) -> GeoDataFusionError {
    GeoArrowError::External(Box::new(error)).into()
}

fn write_wkt_multi_point(
    output: &mut impl fmt::Write,
    multi_point: &impl MultiPointTrait<T = f64>,
) -> std::result::Result<(), wkt::error::Error> {
    write_wkt_prefix(output, "MULTIPOINT", multi_point.dim())?;
    let mut points = multi_point.points();
    let Some(first) = points.next() else {
        output.write_str(" EMPTY")?;
        return Ok(());
    };

    output.write_char('(')?;
    write_wkt_multi_point_member(output, &first, multi_point.dim())?;
    for point in points {
        output.write_char(',')?;
        write_wkt_multi_point_member(output, &point, multi_point.dim())?;
    }
    output.write_char(')')?;
    Ok(())
}

fn write_wkt_multi_point_member(
    output: &mut impl fmt::Write,
    point: &impl PointTrait<T = f64>,
    dimension: geo_traits::Dimensions,
) -> std::result::Result<(), wkt::error::Error> {
    let Some(coord) = point.coord() else {
        output.write_str("EMPTY")?;
        return Ok(());
    };
    output.write_char('(')?;
    write_wkt_coord(output, &coord, dimension)?;
    output.write_char(')')?;
    Ok(())
}

fn write_wkt_multi_polygon(
    output: &mut impl fmt::Write,
    multi_polygon: &impl MultiPolygonTrait<T = f64>,
) -> std::result::Result<(), wkt::error::Error> {
    write_wkt_prefix(output, "MULTIPOLYGON", multi_polygon.dim())?;
    if multi_polygon.num_polygons() == 0 {
        output.write_str(" EMPTY")?;
        return Ok(());
    }

    output.write_char('(')?;
    for (index, polygon) in multi_polygon.polygons().enumerate() {
        if index > 0 {
            output.write_char(',')?;
        }
        let Some(exterior) = polygon.exterior().filter(|ring| ring.num_coords() > 0) else {
            output.write_str("EMPTY")?;
            continue;
        };

        output.write_char('(')?;
        write_wkt_coord_sequence(output, exterior.coords(), multi_polygon.dim())?;
        for interior in polygon.interiors() {
            output.write_char(',')?;
            write_wkt_coord_sequence(output, interior.coords(), multi_polygon.dim())?;
        }
        output.write_char(')')?;
    }
    output.write_char(')')?;
    Ok(())
}

fn write_wkt_geometry_collection(
    output: &mut impl fmt::Write,
    collection: &impl GeometryCollectionTrait<T = f64>,
    depth: usize,
) -> GeoDataFusionResult<()> {
    write_wkt_prefix(output, "GEOMETRYCOLLECTION", collection.dim()).map_err(wkt_error)?;
    if collection.num_geometries() == 0 {
        output
            .write_str(" EMPTY")
            .map_err(|error| wkt_error(error.into()))?;
        return Ok(());
    }

    output
        .write_char('(')
        .map_err(|error| wkt_error(error.into()))?;
    for (index, geometry) in collection.geometries().enumerate() {
        if index > 0 {
            output
                .write_char(',')
                .map_err(|error| wkt_error(error.into()))?;
        }
        write_wkt_geometry_inner(output, &geometry, depth + 1)?;
    }
    output
        .write_char(')')
        .map_err(|error| wkt_error(error.into()))?;
    Ok(())
}

fn write_wkt_coord_sequence<C: CoordTrait<T = f64>>(
    output: &mut impl fmt::Write,
    coords: impl Iterator<Item = C>,
    dimension: geo_traits::Dimensions,
) -> std::result::Result<(), wkt::error::Error> {
    output.write_char('(')?;
    for (index, coord) in coords.enumerate() {
        if index > 0 {
            output.write_char(',')?;
        }
        write_wkt_coord(output, &coord, dimension)?;
    }
    output.write_char(')')?;
    Ok(())
}

fn write_wkt_coord(
    output: &mut impl fmt::Write,
    coord: &impl CoordTrait<T = f64>,
    dimension: geo_traits::Dimensions,
) -> std::result::Result<(), wkt::error::Error> {
    match dimension {
        geo_traits::Dimensions::Xy | geo_traits::Dimensions::Unknown(2) => {
            write!(output, "{} {}", coord.x(), coord.y())?;
        }
        geo_traits::Dimensions::Xyz
        | geo_traits::Dimensions::Xym
        | geo_traits::Dimensions::Unknown(3) => {
            write!(
                output,
                "{} {} {}",
                coord.x(),
                coord.y(),
                coord.nth(2).ok_or(wkt::error::Error::UnknownDimension)?
            )?;
        }
        geo_traits::Dimensions::Xyzm | geo_traits::Dimensions::Unknown(4) => {
            write!(
                output,
                "{} {} {} {}",
                coord.x(),
                coord.y(),
                coord.nth(2).ok_or(wkt::error::Error::UnknownDimension)?,
                coord.nth(3).ok_or(wkt::error::Error::UnknownDimension)?
            )?;
        }
        geo_traits::Dimensions::Unknown(_) => return Err(wkt::error::Error::UnknownDimension),
    }
    Ok(())
}

fn write_wkt_prefix(
    output: &mut impl fmt::Write,
    name: &str,
    dimension: geo_traits::Dimensions,
) -> std::result::Result<(), wkt::error::Error> {
    output.write_str(name)?;
    match dimension {
        geo_traits::Dimensions::Xy | geo_traits::Dimensions::Unknown(2) => {}
        geo_traits::Dimensions::Xyz | geo_traits::Dimensions::Unknown(3) => {
            output.write_str(" Z")?;
        }
        geo_traits::Dimensions::Xym => output.write_str(" M")?,
        geo_traits::Dimensions::Xyzm | geo_traits::Dimensions::Unknown(4) => {
            output.write_str(" ZM")?;
        }
        geo_traits::Dimensions::Unknown(_) => return Err(wkt::error::Error::UnknownDimension),
    }
    Ok(())
}

impl Default for AsText {
    fn default() -> Self {
        Self::new()
    }
}

static AS_TEXT_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for AsText {
    fn name(&self) -> &str {
        "st_astext"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<Field>> {
        let input_field = &args.arg_fields[0];
        let metadata = Arc::new(Metadata::try_from(input_field.as_ref())?);
        let wkb_type = WktType::new(metadata);
        Ok(Field::new(
            input_field.name(),
            DataType::Utf8,
            input_field.is_nullable(),
        )
        .with_extension_type(wkb_type)
        .into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(self.invoke_with_args(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(AS_TEXT_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the OGC Well-Known Text (WKT) representation of the geometry/geography.",
                "ST_AsText(geometry)",
            )
            .with_argument("g1", "geometry")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct GeomFromText {
    signature: Signature,
    coord_type: CoordType,
    aliases: Vec<String>,
}

impl GeomFromText {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View],
                Volatility::Immutable,
            ),
            coord_type,
            aliases: vec!["st_geometryfromtext".to_string(), "st_wkttosql".to_string()],
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let array = &arrays[0];
        let metadata = Arc::new(Metadata::try_from(args.return_field.as_ref())?);
        let to_type = GeometryType::new(metadata).with_coord_type(self.coord_type);
        let geom_arr = decode_wkt_array(array.as_ref(), to_type, None, None)?;

        Ok(ColumnarValue::Array(geom_arr))
    }
}

impl Default for GeomFromText {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static GEOM_FROM_TEXT_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for GeomFromText {
    fn name(&self) -> &str {
        "st_geomfromtext"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<Field>> {
        let input_field = &args.arg_fields[0];
        let metadata = Arc::new(Metadata::try_from(input_field.as_ref())?);
        let geom_type = GeometryType::new(metadata).with_coord_type(self.coord_type);
        Ok(geom_type
            .to_field(input_field.name(), input_field.is_nullable())
            .into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(self.invoke_with_args(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(GEOM_FROM_TEXT_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Constructs a geometry object from the OGC Well-Known text representation.",
                "ST_GeomFromText(text)",
            )
            .with_argument("g1", "geometry")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub(crate) struct TypedGeomFromText {
    name: &'static str,
    signature: Signature,
    coord_type: CoordType,
    target: GeometryKind,
    aliases: Vec<String>,
}

impl TypedGeomFromText {
    pub(crate) fn new(name: &'static str, target: GeometryKind) -> Self {
        Self {
            name,
            signature: typed_geometry_signature(vec![
                DataType::Utf8,
                DataType::LargeUtf8,
                DataType::Utf8View,
            ]),
            coord_type: CoordType::default(),
            target,
            aliases: vec![],
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
        let metadata = Arc::new(Metadata::try_from(args.return_field.as_ref())?);
        let to_type = GeometryType::new(metadata).with_coord_type(self.coord_type);

        if resolve_srid_argument(&args, 1, self.name)?.is_null() {
            return Ok(ColumnarValue::Array(empty_geometry_array(
                to_type,
                args.number_rows,
            )));
        }

        let arrays = ColumnarValue::values_to_arrays(&args.args[..1])?;
        let array = &arrays[0];
        let geom_arr = decode_wkt_array(array.as_ref(), to_type, Some(self.target), None)?;

        Ok(ColumnarValue::Array(geom_arr))
    }
}

static TYPED_GEOM_FROM_TEXT_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for TypedGeomFromText {
    fn name(&self) -> &str {
        self.name
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<Field>> {
        let input_field = &args.arg_fields[0];
        let metadata = typed_metadata_from_args(&args, input_field, self.name)?;
        let geom_type = GeometryType::new(metadata).with_coord_type(self.coord_type);
        Ok(geom_type
            .to_field(input_field.name(), input_field.is_nullable())
            .into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(self.invoke_with_args(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(TYPED_GEOM_FROM_TEXT_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Constructs a geometry from WKT. A valid geometry of the wrong type produces NULL. The optional SRID must be a literal.",
                "ST_PointFromText(wkt[, srid])",
            )
            .with_argument("wkt", "well-known text")
            .with_argument("srid", "optional literal integer SRID")
            .build()
        }))
    }
}

pub(crate) fn decode_wkt_array(
    array: &dyn Array,
    typ: GeometryType,
    target: Option<GeometryKind>,
    srids: Option<(&str, i32, &[Option<i32>])>,
) -> GeoDataFusionResult<ArrayRef> {
    let mut builder = StrictGeometryBuilder::new(typ);
    match array.data_type() {
        DataType::Utf8 => {
            decode_wkt_values(array.as_string::<i32>().iter(), &mut builder, target, srids)?
        }
        DataType::LargeUtf8 => {
            decode_wkt_values(array.as_string::<i64>().iter(), &mut builder, target, srids)?
        }
        DataType::Utf8View => {
            decode_wkt_values(array.as_string_view().iter(), &mut builder, target, srids)?
        }
        other => {
            return Err(DataFusionError::Internal(format!(
                "expected a string array for WKT input, got {other}"
            ))
            .into());
        }
    }
    Ok(builder.finish().into_array_ref())
}

fn decode_wkt_values<'a>(
    values: impl Iterator<Item = Option<&'a str>>,
    builder: &mut StrictGeometryBuilder,
    target: Option<GeometryKind>,
    srids: Option<(&str, i32, &[Option<i32>])>,
) -> GeoDataFusionResult<()> {
    for (row, value) in values.enumerate() {
        let Some(value) = value else {
            builder.push_geometry(None::<&geo_traits::UnimplementedGeometry<f64>>)?;
            continue;
        };
        let geometry = parse_wkt_geometry(value)?;
        push_decoded_geometry(builder, &geometry, target, srids, row)?;
    }
    Ok(())
}

fn parse_wkt_geometry(input: &str) -> GeoDataFusionResult<Wkt<f64>> {
    ensure_wkt_text_depth(input)?;
    match input.parse::<Wkt<f64>>() {
        Ok(geometry) => Ok(geometry),
        Err(original) => match parse_fallback_geometry(input) {
            Ok(Some(geometry)) => Ok(geometry),
            Ok(None) => Err(GeoArrowError::Wkt(original.to_string()).into()),
            Err(message) => Err(GeoArrowError::Wkt(message).into()),
        },
    }
}

fn parse_fallback_geometry(input: &str) -> Result<Option<Wkt<f64>>, String> {
    match parse_multipoint_with_empty_members(input)? {
        Some(geometry) => Ok(Some(geometry)),
        None => parse_geometry_collection_with_empty_members(input),
    }
}

/// Parses `MULTIPOINT(EMPTY, ...)`, which the `wkt` crate rejects even though
/// PostGIS accepts it. Members still go through the same parser one at a time;
/// empty members become empty points instead of being replaced or dropped.
fn parse_multipoint_with_empty_members(input: &str) -> Result<Option<Wkt<f64>>, String> {
    let Some((dimension, body)) = parse_type_body(input, "MULTIPOINT") else {
        return Ok(None);
    };

    let members = split_top_level_members(body)?;
    if !members
        .iter()
        .any(|member| member.trim().eq_ignore_ascii_case("EMPTY"))
    {
        return Ok(None);
    }

    let mut points = Vec::with_capacity(members.len());
    for member in members {
        points.push(parse_multipoint_member(member, dimension)?);
    }
    Ok(Some(Wkt::MultiPoint(WktMultiPoint::new(points, dimension))))
}

/// Rebuilds a GeometryCollection when one of its members uses a legal form the
/// `wkt` crate cannot parse on its own, such as a nested `MULTIPOINT(EMPTY, ...)`.
fn parse_geometry_collection_with_empty_members(input: &str) -> Result<Option<Wkt<f64>>, String> {
    let Some((dimension, body)) = parse_type_body(input, "GEOMETRYCOLLECTION") else {
        return Ok(None);
    };

    let mut geometries = Vec::new();
    for member in split_top_level_members(body)? {
        let member = member.trim();
        if member.is_empty() {
            return Ok(None);
        }
        match parse_wkt_geometry(member) {
            Ok(geometry) => geometries.push(geometry),
            Err(_) => return Ok(None),
        }
    }
    Ok(Some(Wkt::GeometryCollection(WktGeometryCollection::new(
        geometries, dimension,
    ))))
}

fn parse_type_body<'a>(input: &'a str, keyword: &str) -> Option<(WktDimension, &'a str)> {
    let rest = strip_ascii_prefix(input.trim(), keyword)?;
    let (dimension, rest) = match strip_dimension_marker(rest) {
        Some((dimension, rest)) => (dimension, rest),
        None => match strip_dimension_marker(rest.trim_start()) {
            Some((dimension, rest)) => (dimension, rest),
            None => (WktDimension::XY, rest),
        },
    };
    let rest = rest.trim_start().strip_prefix('(')?;
    let (body, trailing) = outer_parenthesized_body(rest)?;
    trailing.trim().is_empty().then_some((dimension, body))
}

fn parse_multipoint_member(member: &str, dimension: WktDimension) -> Result<WktPoint, String> {
    let member = member.trim();
    if member.is_empty() {
        return Err("MULTIPOINT contains an empty member".to_string());
    }
    if member.eq_ignore_ascii_case("EMPTY") {
        return Ok(WktPoint::empty(dimension));
    }

    let coordinates = if member.starts_with('(') && member.ends_with(')') {
        member.to_string()
    } else {
        format!("({member})")
    };
    let marker = match dimension {
        WktDimension::XY => "",
        WktDimension::XYZ => " Z",
        WktDimension::XYM => " M",
        WktDimension::XYZM => " ZM",
    };
    match format!("POINT{marker} {coordinates}").parse::<Wkt<f64>>() {
        Ok(Wkt::Point(point)) => Ok(point),
        Ok(other) => Err(format!("MULTIPOINT member parsed as {other:?}")),
        Err(error) => Err(error.to_string()),
    }
}

fn strip_ascii_prefix<'a>(input: &'a str, prefix: &str) -> Option<&'a str> {
    let head = input.get(..prefix.len())?;
    head.eq_ignore_ascii_case(prefix)
        .then(|| input.get(prefix.len()..).unwrap())
}

fn strip_dimension_marker(input: &str) -> Option<(WktDimension, &str)> {
    let (dimension, length) = match input.as_bytes().first().copied()? {
        b'Z' | b'z' => match input.as_bytes().get(1).copied() {
            Some(b'M' | b'm') => (WktDimension::XYZM, 2),
            _ => (WktDimension::XYZ, 1),
        },
        b'M' | b'm' => (WktDimension::XYM, 1),
        _ => return None,
    };
    let tail = input.get(length..)?;
    let boundary = tail.is_empty()
        || tail.starts_with('(')
        || tail.chars().next().is_some_and(char::is_whitespace);
    boundary.then_some((dimension, tail))
}

fn outer_parenthesized_body(input: &str) -> Option<(&str, &str)> {
    let mut depth = 0usize;
    for (index, character) in input.char_indices() {
        match character {
            '(' => depth += 1,
            ')' if depth == 0 => {
                return Some((&input[..index], &input[index + 1..]));
            }
            ')' => depth -= 1,
            _ => {}
        }
    }
    None
}

fn split_top_level_members(body: &str) -> Result<Vec<&str>, String> {
    let mut members = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    for (index, character) in body.char_indices() {
        match character {
            '(' => depth += 1,
            ')' => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| "MULTIPOINT has unbalanced parentheses".to_string())?;
            }
            ',' if depth == 0 => {
                members.push(&body[start..index]);
                start = index + 1;
            }
            _ => {}
        }
    }
    if depth != 0 {
        return Err("MULTIPOINT has unbalanced parentheses".to_string());
    }
    members.push(&body[start..]);
    Ok(members)
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::RecordBatch;
    use arrow_schema::Schema;
    use datafusion::prelude::SessionContext;
    use geoarrow_array::test::point;
    use geoarrow_schema::crs::Crs;
    use geoarrow_schema::{CoordType, Dimension, Metadata};

    use super::*;

    #[tokio::test]
    async fn test_as_text() {
        let ctx = SessionContext::new();

        let crs = Crs::from_authority_code("EPSG:4326".to_string());
        let metadata = Arc::new(Metadata::new(crs.clone(), Default::default()));

        let geo_arr = point::array(CoordType::Separated, Dimension::XY).with_metadata(metadata);

        let arr = geo_arr.to_array_ref();
        let field = geo_arr.data_type().to_field("geometry", true);
        let schema = Schema::new([Arc::new(field)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![arr]).unwrap();

        ctx.register_batch("t", batch).unwrap();

        ctx.register_udf(AsText::new().into());

        let sql_df = ctx.sql("SELECT ST_AsText(geometry) FROM t;").await.unwrap();

        let output_batches = sql_df.collect().await.unwrap();
        assert_eq!(output_batches.len(), 1);
        let output_batch = &output_batches[0];

        let output_schema = output_batch.schema();
        let output_field = output_schema.field(0);
        let output_wkb_type = output_field.try_extension_type::<WktType>().unwrap();

        assert_eq!(&crs, output_wkb_type.metadata().crs());
    }

    #[tokio::test]
    async fn test_from_text() {
        let ctx = SessionContext::new();

        ctx.register_udf(GeomFromText::new(CoordType::Separated).into());

        let sql_df = ctx
            .sql(r#"SELECT ST_GeomFromText('POINT(30 10)');"#)
            .await
            .unwrap();

        sql_df.show().await.unwrap();
    }

    /// The writer bounds its own recursion so that typed sources which bypass
    /// the WKB reader cannot overflow the formatter stack.
    #[test]
    fn test_write_geometry_bounds_collection_nesting() {
        let mut geometry = Wkt::GeometryCollection(WktGeometryCollection::empty(WktDimension::XY));
        for _ in 0..MAX_WKT_NESTING_DEPTH {
            geometry = Wkt::GeometryCollection(WktGeometryCollection::new(
                vec![geometry],
                WktDimension::XY,
            ));
        }
        write_wkt_geometry(&mut String::new(), &geometry).unwrap();

        geometry =
            Wkt::GeometryCollection(WktGeometryCollection::new(vec![geometry], WktDimension::XY));
        let error = write_wkt_geometry(&mut String::new(), &geometry).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("geometry collection nesting depth exceeds 128"),
            "unexpected error: {error}"
        );
    }

    fn nested_wkt_text(depth: usize) -> String {
        let mut value = String::new();
        for _ in 0..depth {
            value.push_str("GEOMETRYCOLLECTION(");
        }
        value.push_str("LINESTRING(0 0,3 4)");
        for _ in 0..depth {
            value.push(')');
        }
        value
    }

    #[test]
    fn test_wkt_nesting_depth_counts_collections_only() {
        assert_eq!(wkt_nesting_depth(&nested_wkt_text(129)), 129);
        assert_eq!(wkt_nesting_depth("MULTIPOLYGON(((0 0,1 0,1 1,0 0)))"), 0);
        assert_eq!(
            wkt_nesting_depth("GEOMETRYCOLLECTION Z(GEOMETRYCOLLECTIONZM(POINT(1 2)))"),
            2
        );
        assert_eq!(wkt_nesting_depth("GEOMETRYCOLLECTIONZ(POINT(1 2))"), 1);
        assert_eq!(wkt_nesting_depth("),GEOMETRYCOLLECTION("), 1);
        assert_eq!(wkt_nesting_depth("POINT EMPTY"), 0);
    }

    #[test]
    fn test_wkt_text_depth_boundary() {
        ensure_wkt_text_depth(&nested_wkt_text(MAX_WKT_NESTING_DEPTH)).unwrap();
        let error = ensure_wkt_text_depth(&nested_wkt_text(MAX_WKT_NESTING_DEPTH + 1)).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("geometry collection nesting depth exceeds 128"),
            "unexpected error: {error}"
        );
    }
}

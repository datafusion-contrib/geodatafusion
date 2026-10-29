use std::sync::{Arc, OnceLock};

use arrow_array::builder::{BinaryBuilder, StringBuilder};
use arrow_array::{Array, ArrayRef, BinaryArray, LargeStringArray, StringArray, StringViewArray};
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
use geoarrow_array::array::{LargeWkbArray, WkbArray, WkbViewArray};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::{CoordType, GeometryType, Metadata, WkbType};

use super::common::{
    decode_geometry_array_with_srid, present_ewkb_srids, read_ewkb_srid, srid_from_metadata,
    validate_embedded_srids,
};
use crate::data_types::any_geometry_type;
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::{
    expand_rect_to_geometry, field_metadata, geometry_array_from_arrow, metadata_for_srid,
};

fn ewkb_output_signature() -> Signature {
    let geometry_types = any_geometry_type();
    let mut signatures = vec![TypeSignature::Uniform(1, geometry_types.clone())];
    for geometry_type in geometry_types {
        for text_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            signatures.push(TypeSignature::Exact(vec![geometry_type.clone(), text_type]));
        }
    }
    Signature::one_of(signatures, Volatility::Immutable)
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct AsEWKB {
    signature: Signature,
}

impl AsEWKB {
    pub fn new() -> Self {
        Self {
            signature: ewkb_output_signature(),
        }
    }
}

impl Default for AsEWKB {
    fn default() -> Self {
        Self::new()
    }
}

static AS_EWKB_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for AsEWKB {
    fn name(&self) -> &str {
        "st_asewkb"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<Field>> {
        let input_field = &args.arg_fields[0];
        let metadata = output_metadata(
            args.scalar_arguments.first().copied().flatten(),
            input_field,
            self.name(),
        )?;
        let wkb_type = WkbType::new(metadata);
        Ok(Field::new(
            input_field.name(),
            DataType::Binary,
            input_field.is_nullable(),
        )
        .with_extension_type(wkb_type)
        .into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let srid = argument_srid(
            self.name(),
            &args.arg_fields[0],
            args.args.first().expect("ST_AsEWKB requires one argument"),
        )?;
        let binary = write_ewkb_impl(&args, self.name(), srid)?;
        Ok(ColumnarValue::Array(Arc::new(binary)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(AS_EWKB_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the Extended Well-Known Binary (EWKB) representation with SRID metadata.",
                "ST_AsEWKB(geometry[, NDR_or_XDR])",
            )
            .with_argument("g1", "geometry")
            .with_argument("endianness", "optional NDR or XDR")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct AsHEXEWKB {
    signature: Signature,
}

impl AsHEXEWKB {
    pub fn new() -> Self {
        Self {
            signature: ewkb_output_signature(),
        }
    }
}

impl Default for AsHEXEWKB {
    fn default() -> Self {
        Self::new()
    }
}

static AS_HEXEWKB_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for AsHEXEWKB {
    fn name(&self) -> &str {
        "st_ashexewkb"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<Field>> {
        let input_field = &args.arg_fields[0];
        output_metadata(
            args.scalar_arguments.first().copied().flatten(),
            input_field,
            self.name(),
        )?;
        Ok(Arc::new(Field::new(
            input_field.name(),
            DataType::Utf8,
            input_field.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let srid = argument_srid(
            self.name(),
            &args.arg_fields[0],
            args.args
                .first()
                .expect("ST_AsHEXEWKB requires one argument"),
        )?;
        let binary = write_ewkb_impl(&args, self.name(), srid)?;
        let mut builder = StringBuilder::with_capacity(binary.len(), binary.len() * 24);
        for value in binary.iter() {
            match value {
                Some(value) => builder.append_value(hex_encode(value)),
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(AS_HEXEWKB_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns HEXEWKB text using either little-endian (NDR) or big-endian (XDR) encoding.",
                "ST_AsHEXEWKB(geometry[, NDR_or_XDR])",
            )
            .with_argument("g1", "geometry")
            .with_argument("endianness", "optional NDR or XDR")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct GeomFromEWKB {
    coord_type: CoordType,
}

impl GeomFromEWKB {
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
        let array = &ColumnarValue::values_to_arrays(&args.args)?[0];
        let field = &args.arg_fields[0];
        let srids = parser_ewkb_srids(array.as_ref(), field.as_ref())?;
        let metadata = Arc::new(Metadata::try_from(args.return_field.as_ref())?);
        let expected_srid = srid_from_metadata(&metadata, self.name())?;
        let to_type = GeometryType::new(metadata).with_coord_type(self.coord_type);
        let geom_arr = match field.data_type() {
            DataType::Binary => decode_geometry_array_with_srid(
                &WkbArray::try_from((array.as_ref(), field.as_ref()))?,
                to_type,
                None,
                self.name(),
                expected_srid,
                &srids,
            ),
            DataType::LargeBinary => decode_geometry_array_with_srid(
                &LargeWkbArray::try_from((array.as_ref(), field.as_ref()))?,
                to_type,
                None,
                self.name(),
                expected_srid,
                &srids,
            ),
            DataType::BinaryView => decode_geometry_array_with_srid(
                &WkbViewArray::try_from((array.as_ref(), field.as_ref()))?,
                to_type,
                None,
                self.name(),
                expected_srid,
                &srids,
            ),
            _ => unreachable!(),
        }?;
        Ok(ColumnarValue::Array(geom_arr))
    }
}

impl Default for GeomFromEWKB {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static GEOM_FROM_EWKB_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for GeomFromEWKB {
    fn name(&self) -> &str {
        "st_geomfromewkb"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: OnceLock<Signature> = OnceLock::new();
        SIGNATURE.get_or_init(|| {
            Signature::uniform(
                1,
                vec![
                    DataType::Binary,
                    DataType::LargeBinary,
                    DataType::BinaryView,
                ],
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
            Some(ScalarValue::Binary(Some(bytes)))
            | Some(ScalarValue::LargeBinary(Some(bytes)))
            | Some(ScalarValue::BinaryView(Some(bytes))) => {
                metadata_for_srid(read_ewkb_srid(bytes)?.unwrap_or(0))
            }
            Some(ScalarValue::Binary(None))
            | Some(ScalarValue::LargeBinary(None))
            | Some(ScalarValue::BinaryView(None))
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
        Some(GEOM_FROM_EWKB_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Constructs a geometry from Extended Well-Known Binary (EWKB).",
                "ST_GeomFromEWKB(ewkb)",
            )
            .with_argument("ewkb", "extended well-known binary")
            .build()
        }))
    }
}

fn output_metadata(
    scalar: Option<&ScalarValue>,
    input_field: &Field,
    function_name: &str,
) -> Result<Arc<Metadata>> {
    match scalar {
        Some(ScalarValue::Binary(Some(bytes)))
        | Some(ScalarValue::LargeBinary(Some(bytes)))
        | Some(ScalarValue::BinaryView(Some(bytes))) => {
            Ok(metadata_for_srid(read_ewkb_srid(bytes)?.unwrap_or(0)))
        }
        Some(ScalarValue::Binary(None))
        | Some(ScalarValue::LargeBinary(None))
        | Some(ScalarValue::BinaryView(None))
        | Some(ScalarValue::Null) => Ok(metadata_for_srid(0)),
        _ => {
            let metadata = field_metadata(input_field)?;
            srid_from_metadata(&metadata, function_name)?;
            Ok(metadata)
        }
    }
}

fn argument_srid(function_name: &str, field: &Field, arg: &ColumnarValue) -> Result<i32> {
    if let ColumnarValue::Scalar(ScalarValue::Binary(Some(bytes)))
    | ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(bytes)))
    | ColumnarValue::Scalar(ScalarValue::BinaryView(Some(bytes))) = arg
    {
        return Ok(read_ewkb_srid(bytes)?.unwrap_or(0));
    }
    let metadata = field_metadata(field)?;
    Ok(srid_from_metadata(&metadata, function_name)?)
}

fn parser_ewkb_srids(array: &dyn Array, field: &Field) -> Result<Vec<Option<i32>>> {
    let values = match field.data_type() {
        DataType::Binary => array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .iter()
            .map(|value| {
                value
                    .map(|bytes| read_ewkb_srid(bytes).map(|srid| srid.unwrap_or(0)))
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()?,
        DataType::LargeBinary => array
            .as_any()
            .downcast_ref::<arrow_array::LargeBinaryArray>()
            .unwrap()
            .iter()
            .map(|value| {
                value
                    .map(|bytes| read_ewkb_srid(bytes).map(|srid| srid.unwrap_or(0)))
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()?,
        DataType::BinaryView => array
            .as_any()
            .downcast_ref::<arrow_array::BinaryViewArray>()
            .unwrap()
            .iter()
            .map(|value| {
                value
                    .map(|bytes| read_ewkb_srid(bytes).map(|srid| srid.unwrap_or(0)))
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()?,
        _ => unreachable!(),
    };
    Ok(values)
}

fn write_ewkb_impl(
    args: &ScalarFunctionArgs,
    function_name: &str,
    srid: i32,
) -> Result<BinaryArray> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    if let Some(embedded_srids) = present_ewkb_srids(&arrays[0], &args.arg_fields[0])? {
        validate_embedded_srids(function_name, srid, &embedded_srids)?;
    }
    let geo_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
    let endianness = EndianColumn::from_array(arrays.get(1))?;
    let expanded = expand_rect_to_geometry(geo_array.as_ref())?;
    let geo_array: &dyn GeoArrowArray = expanded
        .as_ref()
        .map_or(geo_array.as_ref(), |expanded| expanded);
    Ok(downcast_geoarrow_array!(
        geo_array,
        write_ewkb_array,
        &endianness,
        srid
    )?)
}

enum EndianColumn<'a> {
    Default,
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Utf8View(&'a StringViewArray),
}

impl<'a> EndianColumn<'a> {
    fn from_array(array: Option<&'a ArrayRef>) -> Result<Self> {
        let Some(array) = array else {
            return Ok(Self::Default);
        };
        match array.data_type() {
            DataType::Utf8 => Ok(Self::Utf8(array.as_any().downcast_ref().unwrap())),
            DataType::LargeUtf8 => Ok(Self::LargeUtf8(array.as_any().downcast_ref().unwrap())),
            DataType::Utf8View => Ok(Self::Utf8View(array.as_any().downcast_ref().unwrap())),
            other => Err(DataFusionError::Internal(format!(
                "unexpected EWKB endianness type {other}"
            ))),
        }
    }

    fn is_xdr(&self, row: usize) -> Option<bool> {
        match self {
            Self::Default => Some(false),
            Self::Utf8(array) => (!array.is_null(row)).then(|| is_xdr(array.value(row))),
            Self::LargeUtf8(array) => (!array.is_null(row)).then(|| is_xdr(array.value(row))),
            Self::Utf8View(array) => (!array.is_null(row)).then(|| is_xdr(array.value(row))),
        }
    }
}

fn is_xdr(value: &str) -> bool {
    // PostGIS accepts only these two spellings, and only as a prefix.
    value.starts_with("XDR") || value.starts_with("xdr")
}

#[derive(Clone, Copy)]
enum Endian {
    Little,
    Big,
}

fn write_ewkb_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    endianness: &EndianColumn<'_>,
    srid: i32,
) -> GeoDataFusionResult<BinaryArray> {
    let mut builder = BinaryBuilder::with_capacity(array.len(), array.len() * 32);
    for (row, item) in array.iter().enumerate() {
        let Some(xdr) = endianness.is_xdr(row) else {
            builder.append_null();
            continue;
        };
        match item {
            None => builder.append_null(),
            Some(Err(err)) => return Err(err.into()),
            Some(Ok(geometry)) => {
                let mut bytes = Vec::new();
                write_ewkb(
                    &mut bytes,
                    &geometry,
                    if xdr { Endian::Big } else { Endian::Little },
                    (srid != 0).then_some(srid),
                )?;
                builder.append_value(bytes);
            }
        }
    }
    Ok(builder.finish())
}

fn write_ewkb(
    output: &mut Vec<u8>,
    geometry: &impl GeometryTrait<T = f64>,
    endian: Endian,
    root_srid: Option<i32>,
) -> Result<()> {
    let kind = geometry_kind(geometry)?;
    let dim = geometry.dim();
    let mut type_code = kind as u32;
    if matches!(
        dim,
        geo_traits::Dimensions::Xyz | geo_traits::Dimensions::Xyzm
    ) {
        type_code |= 0x8000_0000;
    }
    if matches!(
        dim,
        geo_traits::Dimensions::Xym | geo_traits::Dimensions::Xyzm
    ) {
        type_code |= 0x4000_0000;
    }
    if root_srid.is_some() {
        type_code |= 0x2000_0000;
    }

    output.push(if matches!(endian, Endian::Big) { 0 } else { 1 });
    write_u32(output, type_code, endian);
    if let Some(srid) = root_srid {
        write_i32(output, srid, endian);
    }

    match geometry.as_type() {
        NativeGeometryType::Point(point) => write_point(output, point, &dim, endian),
        NativeGeometryType::LineString(line_string) => {
            write_linestring(output, line_string, &dim, endian)
        }
        NativeGeometryType::Polygon(polygon) => write_polygon(output, polygon, &dim, endian),
        NativeGeometryType::MultiPoint(multi_point) => {
            write_multi_point(output, multi_point, endian)
        }
        NativeGeometryType::MultiLineString(multi_line_string) => {
            write_multi_linestring(output, multi_line_string, endian)
        }
        NativeGeometryType::MultiPolygon(multi_polygon) => {
            write_multi_polygon(output, multi_polygon, endian)
        }
        NativeGeometryType::GeometryCollection(collection) => {
            write_geometry_collection(output, collection, endian)
        }
        NativeGeometryType::Rect(_)
        | NativeGeometryType::Triangle(_)
        | NativeGeometryType::Line(_) => Err(DataFusionError::NotImplemented(
            "ST_AsEWKB only supports the seven GeoArrow geometry families".to_string(),
        )),
    }
}

fn geometry_kind(geometry: &impl GeometryTrait<T = f64>) -> Result<u32> {
    Ok(match geometry.as_type() {
        NativeGeometryType::Point(_) => 1,
        NativeGeometryType::LineString(_) => 2,
        NativeGeometryType::Polygon(_) => 3,
        NativeGeometryType::MultiPoint(_) => 4,
        NativeGeometryType::MultiLineString(_) => 5,
        NativeGeometryType::MultiPolygon(_) => 6,
        NativeGeometryType::GeometryCollection(_) => 7,
        NativeGeometryType::Rect(_)
        | NativeGeometryType::Triangle(_)
        | NativeGeometryType::Line(_) => {
            return Err(DataFusionError::NotImplemented(
                "ST_AsEWKB only supports the seven GeoArrow geometry families".to_string(),
            ));
        }
    })
}

fn write_point(
    output: &mut Vec<u8>,
    point: &impl PointTrait<T = f64>,
    dim: &geo_traits::Dimensions,
    endian: Endian,
) -> Result<()> {
    if let Some(coord) = point.coord() {
        write_coord(output, &coord, *dim, endian);
    } else {
        for _ in 0..dim.size() {
            write_f64(output, f64::NAN, endian);
        }
    }
    Ok(())
}

fn write_linestring(
    output: &mut Vec<u8>,
    line_string: &impl LineStringTrait<T = f64>,
    dim: &geo_traits::Dimensions,
    endian: Endian,
) -> Result<()> {
    let coords = line_string.coords().collect::<Vec<_>>();
    write_u32(output, coords.len() as u32, endian);
    for coord in coords {
        write_coord(output, &coord, *dim, endian);
    }
    Ok(())
}

fn write_polygon(
    output: &mut Vec<u8>,
    polygon: &impl PolygonTrait<T = f64>,
    dim: &geo_traits::Dimensions,
    endian: Endian,
) -> Result<()> {
    let rings = polygon
        .exterior()
        .into_iter()
        .chain(polygon.interiors())
        .collect::<Vec<_>>();
    write_u32(output, rings.len() as u32, endian);
    for ring in rings {
        let coords = ring.coords().collect::<Vec<_>>();
        write_u32(output, coords.len() as u32, endian);
        for coord in coords {
            write_coord(output, &coord, *dim, endian);
        }
    }
    Ok(())
}

fn write_multi_point(
    output: &mut Vec<u8>,
    multi_point: &impl MultiPointTrait<T = f64>,
    endian: Endian,
) -> Result<()> {
    write_u32(output, multi_point.num_points() as u32, endian);
    for point in multi_point.points() {
        write_ewkb(output, &point, endian, None)?;
    }
    Ok(())
}

fn write_multi_linestring(
    output: &mut Vec<u8>,
    multi_line_string: &impl MultiLineStringTrait<T = f64>,
    endian: Endian,
) -> Result<()> {
    write_u32(output, multi_line_string.num_line_strings() as u32, endian);
    for line_string in multi_line_string.line_strings() {
        write_ewkb(output, &line_string, endian, None)?;
    }
    Ok(())
}

fn write_multi_polygon(
    output: &mut Vec<u8>,
    multi_polygon: &impl MultiPolygonTrait<T = f64>,
    endian: Endian,
) -> Result<()> {
    write_u32(output, multi_polygon.num_polygons() as u32, endian);
    for polygon in multi_polygon.polygons() {
        write_ewkb(output, &polygon, endian, None)?;
    }
    Ok(())
}

fn write_geometry_collection(
    output: &mut Vec<u8>,
    collection: &impl GeometryCollectionTrait<T = f64>,
    endian: Endian,
) -> Result<()> {
    write_u32(output, collection.num_geometries() as u32, endian);
    for geometry in collection.geometries() {
        write_ewkb(output, &geometry, endian, None)?;
    }
    Ok(())
}

fn write_coord(
    output: &mut Vec<u8>,
    coord: &impl CoordTrait<T = f64>,
    dim: geo_traits::Dimensions,
    endian: Endian,
) {
    for index in 0..dim.size() {
        write_f64(output, coord.nth_or_panic(index), endian);
    }
}

fn write_u32(output: &mut Vec<u8>, value: u32, endian: Endian) {
    match endian {
        Endian::Little => output.extend_from_slice(&value.to_le_bytes()),
        Endian::Big => output.extend_from_slice(&value.to_be_bytes()),
    }
}

fn write_i32(output: &mut Vec<u8>, value: i32, endian: Endian) {
    match endian {
        Endian::Little => output.extend_from_slice(&value.to_le_bytes()),
        Endian::Big => output.extend_from_slice(&value.to_be_bytes()),
    }
}

fn write_f64(output: &mut Vec<u8>, value: f64, endian: Endian) {
    match endian {
        Endian::Little => output.extend_from_slice(&value.to_le_bytes()),
        Endian::Big => output.extend_from_slice(&value.to_be_bytes()),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn hex_encoding_is_uppercase() {
        assert_eq!(hex_encode(&[0x01, 0xab, 0xff]), "01ABFF");
    }
}

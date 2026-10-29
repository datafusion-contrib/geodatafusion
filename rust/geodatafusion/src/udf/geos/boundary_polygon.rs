//! Boundary-line polygon constructors backed by GEOS `build_area`.

use std::str::FromStr;
use std::sync::Arc;

use arrow_array::{Array, BinaryArray};
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::scalar::ScalarValue;
use geoarrow_schema::{CoordType, GeoArrowType, GeometryType, Metadata};
use geos::{Geom, Geometry as GeosGeometry, GeometryTypes};
use wkt::Wkt;

use super::adapter;
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::{metadata_for_srid, normalize_srid};

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct BdPolyFromText {
    signature: Signature,
}

impl BdPolyFromText {
    pub fn new() -> Self {
        Self {
            signature: polygon_from_text_signature(),
        }
    }
}

impl Default for BdPolyFromText {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for BdPolyFromText {
    fn name(&self) -> &str {
        "st_bdpolyfromtext"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        adapter::check_arg_count(arg_types, &[2], self.name())?;
        Ok(vec![
            adapter::coerce_text(&arg_types[0], 0, self.name())?,
            adapter::coerce_int64(&arg_types[1], 1, self.name())?,
        ])
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let metadata = srid_metadata(&args)?;
        Ok(Arc::new(
            GeometryType::new(metadata)
                .with_coord_type(CoordType::Separated)
                .to_field("", true),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        build_area_impl(&args, self.name(), true)
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct BdMPolyFromText {
    signature: Signature,
}

impl BdMPolyFromText {
    pub fn new() -> Self {
        Self {
            signature: polygon_from_text_signature(),
        }
    }
}

impl Default for BdMPolyFromText {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for BdMPolyFromText {
    fn name(&self) -> &str {
        "st_bdmpolyfromtext"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        adapter::check_arg_count(arg_types, &[2], self.name())?;
        Ok(vec![
            adapter::coerce_text(&arg_types[0], 0, self.name())?,
            adapter::coerce_int64(&arg_types[1], 1, self.name())?,
        ])
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let metadata = srid_metadata(&args)?;
        Ok(Arc::new(
            GeometryType::new(metadata)
                .with_coord_type(CoordType::Separated)
                .to_field("", true),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        build_area_impl(&args, self.name(), false)
    }
}

fn polygon_from_text_signature() -> Signature {
    let mut variants = Vec::new();
    for text_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        variants.push(TypeSignature::Exact(vec![
            text_type.clone(),
            DataType::Int32,
        ]));
        variants.push(TypeSignature::Exact(vec![text_type, DataType::Int64]));
    }
    Signature::one_of(variants, Volatility::Immutable)
}

fn srid_metadata(args: &ReturnFieldArgs) -> Result<Arc<Metadata>> {
    match args.scalar_arguments.get(1) {
        Some(Some(ScalarValue::Int32(Some(value)))) => {
            Ok(metadata_for_srid(normalize_srid(i64::from(*value))?))
        }
        Some(Some(ScalarValue::Int64(Some(value)))) => {
            Ok(metadata_for_srid(normalize_srid(*value)?))
        }
        Some(Some(ScalarValue::Int32(None) | ScalarValue::Int64(None) | ScalarValue::Null)) => {
            Ok(metadata_for_srid(0))
        }
        Some(Some(value)) => Err(DataFusionError::Internal(format!(
            "ST_BdPolyFromText received an unsupported SRID value: {value:?}"
        ))),
        Some(None) | None => Ok(metadata_for_srid(0)),
    }
}

fn build_area_impl(
    args: &ScalarFunctionArgs,
    name: &str,
    single_polygon: bool,
) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    if matches!(args.args.get(1), Some(ColumnarValue::Array(_))) {
        return Err(DataFusionError::Execution(
            "SRID must be a literal because GeoArrow stores CRS in field metadata".to_string(),
        ));
    }

    let text = arrays[0]
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .ok_or_else(|| {
            DataFusionError::Internal(format!("{name} input was not coerced to Utf8"))
        })?;
    let srid = arrays[1]
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .ok_or_else(|| {
            DataFusionError::Internal(format!("{name} SRID was not coerced to Int64"))
        })?;
    let output_type = GeoArrowType::from_arrow_field(args.return_field.as_ref())
        .map_err(crate::error::GeoDataFusionError::GeoArrow)?;
    let mut output = Vec::with_capacity(text.len());
    for row in 0..text.len() {
        if text.is_null(row) || srid.is_null(row) {
            output.push(None);
            continue;
        }
        output.push(build_area_row(text.value(row), name, single_polygon)?);
    }
    Ok(adapter::finish_geos_result(
        BinaryArray::from_iter(output),
        output_type,
    )?)
}

fn build_area_row(
    wkt: &str,
    name: &str,
    single_polygon: bool,
) -> GeoDataFusionResult<Option<Vec<u8>>> {
    let geometry = parse_geos_wkt(wkt, name)?;
    if geometry.geometry_type()? != GeometryTypes::MultiLineString {
        return Err(
            DataFusionError::Execution(format!("{name} input must be a MULTILINESTRING")).into(),
        );
    }

    if geometry.is_empty()? {
        let empty = GeosGeometry::new_from_wkt(if single_polygon {
            "POLYGON EMPTY"
        } else {
            "MULTIPOLYGON EMPTY"
        })?;
        let mut writer = adapter::GeosWkbWriter::new()?;
        return Ok(Some(writer.write(&empty)?));
    }

    let mut built = geometry.build_area()?;
    if built.is_empty()? {
        return Ok(None);
    }
    built.normalize()?;
    let result = match built.geometry_type()? {
        GeometryTypes::Polygon if single_polygon => built,
        GeometryTypes::MultiPolygon if !single_polygon => built,
        GeometryTypes::Polygon => GeosGeometry::create_multipolygon(vec![built])?,
        GeometryTypes::MultiPolygon => {
            if built.get_num_geometries()? != 1 {
                return Err(DataFusionError::Execution(format!(
                    "{name} input returns more than a single polygon"
                ))
                .into());
            }
            Geom::clone(&built.get_geometry_n(0)?)?
        }
        _ => {
            return Err(DataFusionError::Execution(format!(
                "{name} did not produce a polygonal result"
            ))
            .into());
        }
    };
    let mut writer = adapter::GeosWkbWriter::new()?;
    Ok(Some(writer.write(&result)?))
}

fn parse_geos_wkt(wkt: &str, name: &str) -> GeoDataFusionResult<GeosGeometry> {
    let parsed = Wkt::<f64>::from_str(wkt).map_err(|error| {
        DataFusionError::Execution(format!("{name} could not parse WKT: {error}"))
    })?;
    let mut bytes = Vec::new();
    wkb::writer::write_geometry(&mut bytes, &parsed, &Default::default()).map_err(|error| {
        DataFusionError::Execution(format!("{name} could not encode WKT: {error}"))
    })?;
    let prepared = adapter::prepare_geos_wkb(&bytes)?;
    Ok(GeosGeometry::new_from_wkb(prepared.as_ref())?)
}

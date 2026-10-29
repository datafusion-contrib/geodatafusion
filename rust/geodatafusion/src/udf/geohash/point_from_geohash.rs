use std::sync::{Arc, OnceLock};

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, StringArrayType};
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use geo::Coord;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::PointArray;
use geoarrow_array::builder::PointBuilder;
use geoarrow_schema::{CoordType, Dimension, Metadata, PointType};

use super::geohash_codec::{coerce_text_and_precision, decode_bounds};
use crate::error::GeoDataFusionResult;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct PointFromGeoHash {
    signature: Signature,
    coord_type: CoordType,
}

impl PointFromGeoHash {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            coord_type,
        }
    }
}

impl Default for PointFromGeoHash {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for PointFromGeoHash {
    fn name(&self) -> &str {
        "st_pointfromgeohash"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_text_and_precision(arg_types, "ST_PointFromGeoHash")
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "return_field_from_args computes ST_PointFromGeoHash output".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field_impl(args, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(point_from_geohash_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Return a point from a GeoHash string. The point represents the center point of the GeoHash. The optional precision selects the number of GeoHash characters to decode.",
                "ST_PointFromGeoHash(geohash[, precision])",
            )
            .with_argument("text", "geohash")
            .with_argument("precision", "optional number of GeoHash characters")
            .build()
        }))
    }
}

fn return_field_impl(
    args: ReturnFieldArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<FieldRef> {
    let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
    let output_type =
        PointType::new(Dimension::XY, Arc::new(Metadata::default())).with_coord_type(coord_type);
    Ok(Arc::new(
        output_type.to_field(args.arg_fields[0].name().clone(), nullable),
    ))
}

fn point_from_geohash_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let length = args.number_rows;

    if arrays[0].data_type() == &DataType::Null {
        let typ = args.return_field.extension_type::<PointType>();
        let mut builder = PointBuilder::with_capacity(typ, length);
        for _ in 0..length {
            builder.push_null();
        }
        return Ok(ColumnarValue::Array(builder.finish().into_array_ref()));
    }

    let typ = args.return_field.extension_type::<PointType>();
    let precision = arrays.get(1).cloned();
    let result = match arrays[0].data_type() {
        DataType::Utf8 => build_point_arr(typ, &arrays[0].as_string::<i32>(), precision.as_ref())?,
        DataType::LargeUtf8 => {
            build_point_arr(typ, &arrays[0].as_string::<i64>(), precision.as_ref())?
        }
        DataType::Utf8View => {
            build_point_arr(typ, &arrays[0].as_string_view(), precision.as_ref())?
        }
        other => {
            return Err(DataFusionError::Execution(format!(
                "ST_PointFromGeoHash received unexpected type {other}"
            ))
            .into());
        }
    };

    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn build_point_arr<'a>(
    typ: PointType,
    array: &impl StringArrayType<'a>,
    precision: Option<&ArrayRef>,
) -> GeoDataFusionResult<PointArray> {
    let mut builder = PointBuilder::with_capacity(typ, array.len());
    for (row, value) in array.iter().enumerate() {
        let Some(value) = value else {
            builder.push_null();
            continue;
        };
        let precision = precision.and_then(|array| int_value(array, row));
        let bounds = decode_bounds(value, precision).map_err(DataFusionError::Execution)?;
        let (x, y) = bounds.center();
        builder.push_coord(Some(&Coord { x, y }));
    }
    Ok(builder.finish())
}

fn int_value(array: &ArrayRef, row: usize) -> Option<i64> {
    if array.is_null(row) {
        return None;
    }
    match array.data_type() {
        DataType::Int64 => Some(
            array
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(row),
        ),
        DataType::Int32 => Some(i64::from(
            array
                .as_primitive::<arrow_array::types::Int32Type>()
                .value(row),
        )),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use approx::relative_eq;
    use datafusion::prelude::SessionContext;
    use geo_traits::{CoordTrait, PointTrait};
    use geoarrow_array::GeoArrowArrayAccessor;
    use geoarrow_array::array::PointArray;

    use super::*;

    #[tokio::test]
    async fn test_point_from_geohash() {
        let ctx = SessionContext::new();
        ctx.register_udf(PointFromGeoHash::default().into());

        let df = ctx
            .sql("SELECT ST_PointFromGeoHash('9qqj');")
            .await
            .unwrap();

        let schema = df.schema().clone();
        let batches = df.collect().await.unwrap();
        let column = batches[0].column(0);

        let point_array =
            PointArray::try_from((column.as_ref(), schema.field(0).as_ref())).unwrap();
        let point = point_array.value(0).unwrap();

        assert!(relative_eq!(point.coord().unwrap().x(), -115.13671875));
        assert!(relative_eq!(point.coord().unwrap().y(), 36.123046875));
    }
}

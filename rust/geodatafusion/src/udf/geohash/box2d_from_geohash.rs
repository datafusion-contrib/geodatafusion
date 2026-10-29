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
use geo::{Coord, Rect};
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::RectArray;
use geoarrow_array::builder::RectBuilder;
use geoarrow_schema::{BoxType, Dimension, Metadata};

use super::geohash_codec::{coerce_text_and_precision, decode_bounds};
use crate::error::GeoDataFusionResult;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Box2DFromGeoHash;

impl Box2DFromGeoHash {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for Box2DFromGeoHash {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Box2DFromGeoHash {
    fn name(&self) -> &str {
        "st_box2dfromgeohash"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: OnceLock<Signature> = OnceLock::new();
        SIGNATURE.get_or_init(|| Signature::user_defined(Volatility::Immutable))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_text_and_precision(arg_types, "ST_Box2dFromGeoHash")
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "return_field_from_args computes ST_Box2dFromGeoHash output".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field_impl(args)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(box_from_geohash_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Return a BOX2D from a GeoHash string. The optional precision selects the number of GeoHash characters to decode.",
                "ST_Box2dFromGeoHash(geohash[, precision])",
            )
            .with_argument("text", "geohash")
            .with_argument("precision", "optional number of GeoHash characters")
            .build()
        }))
    }
}

fn return_field_impl(args: ReturnFieldArgs) -> GeoDataFusionResult<FieldRef> {
    let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
    let output_type = BoxType::new(Dimension::XY, Arc::new(Metadata::default()));
    Ok(Arc::new(
        output_type.to_field(args.arg_fields[0].name().clone(), nullable),
    ))
}

fn box_from_geohash_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let length = args.number_rows;
    let typ = args.return_field.extension_type::<BoxType>();

    if arrays[0].data_type() == &DataType::Null {
        let mut builder = RectBuilder::with_capacity(typ, length);
        for _ in 0..length {
            builder.push_null();
        }
        return Ok(ColumnarValue::Array(builder.finish().into_array_ref()));
    }

    let precision = arrays.get(1).cloned();
    let result = match arrays[0].data_type() {
        DataType::Utf8 => build_rect_arr(typ, &arrays[0].as_string::<i32>(), precision.as_ref())?,
        DataType::LargeUtf8 => {
            build_rect_arr(typ, &arrays[0].as_string::<i64>(), precision.as_ref())?
        }
        DataType::Utf8View => build_rect_arr(typ, &arrays[0].as_string_view(), precision.as_ref())?,
        other => {
            return Err(DataFusionError::Execution(format!(
                "ST_Box2dFromGeoHash received unexpected type {other}"
            ))
            .into());
        }
    };

    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn build_rect_arr<'a>(
    typ: BoxType,
    array: &impl StringArrayType<'a>,
    precision: Option<&ArrayRef>,
) -> GeoDataFusionResult<RectArray> {
    let mut builder = RectBuilder::with_capacity(typ, array.len());
    for (row, value) in array.iter().enumerate() {
        let Some(value) = value else {
            builder.push_null();
            continue;
        };
        let precision = precision.and_then(|array| int_value(array, row));
        let bounds = decode_bounds(value, precision).map_err(DataFusionError::Execution)?;
        let rect = Rect::new(
            Coord {
                x: bounds.min_lon,
                y: bounds.min_lat,
            },
            Coord {
                x: bounds.max_lon,
                y: bounds.max_lat,
            },
        );
        builder.push_rect(Some(&rect));
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
    use geo_traits::{CoordTrait, RectTrait};
    use geoarrow_array::GeoArrowArrayAccessor;

    use super::*;

    #[tokio::test]
    async fn test_box2d_from_geohash() {
        let ctx = SessionContext::new();
        ctx.register_udf(Box2DFromGeoHash.into());

        let df = ctx
            .sql("SELECT ST_Box2dFromGeoHash('ww8p1r4t8');")
            .await
            .unwrap();

        let schema = df.schema().clone();
        let batches = df.collect().await.unwrap();
        let column = batches[0].column(0);

        let rect_array = RectArray::try_from((column.as_ref(), schema.field(0).as_ref())).unwrap();
        let rect = rect_array.value(0).unwrap();

        assert!(relative_eq!(rect.min().x(), 112.55836486816406));
        assert!(relative_eq!(rect.min().y(), 37.83236503601074));
        assert!(relative_eq!(rect.max().x(), 112.5584077835083));
        assert!(relative_eq!(rect.max().y(), 37.83240795135498));
    }
}

use std::sync::{Arc, OnceLock};

use arrow_array::ArrayRef;
use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::common::scalar::ScalarStructBuilder;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Documentation, Signature};
use datafusion::scalar::ScalarValue;
use geoarrow_array::array::from_arrow_array;
use geoarrow_schema::{BoxType, Dimension, Metadata};

use crate::data_types::any_single_geometry_type_input;
use crate::error::{GeoDataFusionError, GeoDataFusionResult};
use crate::udf::native::bounding_box::util::bounds::total_bounds;

/// `ST_Extent` aggregate UDF implementation: computes the 2D `box2d` bounding box enclosing a set of geometries.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Extent;

impl Extent {
    /// Creates a new `ST_Extent` aggregate UDF instance.
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for Extent {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl AggregateUDFImpl for Extent {
    fn name(&self) -> &str {
        "st_extent"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        Ok(return_field_impl(arg_fields)?)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let input_field = acc_args.exprs[0].return_field(acc_args.schema)?;
        Ok(Box::new(ExtentAccumulator::new(
            input_field,
            acc_args.return_field,
        )))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(["xmin", "ymin", "xmax", "ymax"]
            .into_iter()
            .map(|name| {
                Arc::new(Field::new(
                    format_state_name(args.name, name),
                    DataType::Float64,
                    true,
                ))
            })
            .collect())
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "An aggregate function that returns the bounding box that bounds rows of geometries.",
                "ST_Extent(geomset)",
            )
            .with_argument("geomset", "geometry")
            .build()
        }))
    }
}

fn return_field_impl(args: &[FieldRef]) -> GeoDataFusionResult<FieldRef> {
    let metadata = Arc::new(Metadata::try_from(args[0].as_ref())?);
    let output_type = BoxType::new(Dimension::XY, metadata);
    Ok(Arc::new(output_type.to_field("", true)))
}

#[derive(Debug)]
struct ExtentAccumulator {
    field: FieldRef,
    return_field: FieldRef,
    bounds: Option<[f64; 4]>,
}

impl ExtentAccumulator {
    pub fn new(field: FieldRef, return_field: FieldRef) -> Self {
        Self {
            field,
            return_field,
            bounds: None,
        }
    }

    fn update_bounds(&mut self, bounds: [f64; 4]) {
        self.bounds = Some(match self.bounds {
            Some(current) => [
                current[0].min(bounds[0]),
                current[1].min(bounds[1]),
                current[2].max(bounds[2]),
                current[3].max(bounds[3]),
            ],
            None => bounds,
        });
    }
}

impl Accumulator for ExtentAccumulator {
    /// Intermediate state of the accumulator.
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(match self.bounds {
            Some(bounds) => bounds.into_iter().map(|value| value.into()).collect(),
            None => vec![
                ScalarValue::Float64(None),
                ScalarValue::Float64(None),
                ScalarValue::Float64(None),
                ScalarValue::Float64(None),
            ],
        })
    }

    /// Finish the accumulator
    fn evaluate(&mut self) -> Result<ScalarValue> {
        let Some([xmin, ymin, xmax, ymax]) = self.bounds else {
            return ScalarValue::try_from(self.return_field.data_type());
        };
        let DataType::Struct(fields) = self.return_field.data_type() else {
            return Err(DataFusionError::Internal(format!(
                "ST_Extent expected a struct return type, got {}",
                self.return_field.data_type()
            )));
        };

        ScalarStructBuilder::new()
            .with_scalar(Arc::clone(&fields[0]), xmin.into())
            .with_scalar(Arc::clone(&fields[1]), ymin.into())
            .with_scalar(Arc::clone(&fields[2]), xmax.into())
            .with_scalar(Arc::clone(&fields[3]), ymax.into())
            .build()
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let value = values.first().ok_or_else(|| {
            DataFusionError::Internal("ST_Extent received no input batch".to_string())
        })?;
        let geo_arr = from_arrow_array(value, &self.field).map_err(GeoDataFusionError::GeoArrow)?;
        let bounds = total_bounds(&geo_arr).map_err(GeoDataFusionError::GeoArrow)?;
        let bounds = [bounds.minx(), bounds.miny(), bounds.maxx(), bounds.maxy()];
        if bounds.into_iter().all(f64::is_finite) {
            self.update_bounds(bounds);
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.len() != 4 {
            return Err(DataFusionError::Internal(format!(
                "ST_Extent expected four state arrays, got {}",
                states.len()
            )));
        }

        let state_array = |index: usize| -> Result<&arrow_array::PrimitiveArray<Float64Type>> {
            states[index]
                .as_primitive_opt::<Float64Type>()
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "ST_Extent state field {index} has type {}, expected Float64",
                        states[index].data_type()
                    ))
                })
        };
        let merged = [
            arrow_arith::aggregate::min(state_array(0)?),
            arrow_arith::aggregate::min(state_array(1)?),
            arrow_arith::aggregate::max(state_array(2)?),
            arrow_arith::aggregate::max(state_array(3)?),
        ];
        if let [Some(xmin), Some(ymin), Some(xmax), Some(ymax)] = merged
            && [xmin, ymin, xmax, ymax].into_iter().all(f64::is_finite)
        {
            self.update_bounds([xmin, ymin, xmax, ymax]);
        }

        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

#[cfg(test)]
mod test {
    // use arrow_array::cast::AsArray;
    // use arrow_array::types::Float64Type;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::udf::native::io::GeomFromText;

    #[tokio::test]
    async fn test() {
        let ctx = SessionContext::new();

        ctx.register_udaf(Extent::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql(
                "select ST_Extent(ST_GeomFromText('POLYGON((743238 2967416,743238 2967450,
				 743265 2967450,743265.625 2967416,743238 2967416))'));",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        let struct_arr = col.as_struct();
        assert_eq!(
            struct_arr.column(0).as_primitive::<Float64Type>().value(0),
            743238.0
        );
        assert_eq!(
            struct_arr.column(1).as_primitive::<Float64Type>().value(0),
            2967416.0
        );
        assert_eq!(
            struct_arr.column(2).as_primitive::<Float64Type>().value(0),
            743265.625
        );
        assert_eq!(
            struct_arr.column(3).as_primitive::<Float64Type>().value(0),
            2967450.0
        );
    }
}

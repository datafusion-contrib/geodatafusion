use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::common::scalar::ScalarStructBuilder;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature};
use datafusion::scalar::ScalarValue;
use geoarrow_schema::{BoxType, Dimension};

use crate::data_types::any_single_geometry_type_input;
use crate::error::{GeoDataFusionError, GeoDataFusionResult};
use crate::udf::native::bounding_box::util::bounds::{
    Extent3DBounds, total_bounds, total_bounds_3d,
};
use crate::udf::native::geometry::{
    field_metadata, geometry_array_from_arrow, is_null_geometry_input,
};

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Extent;

impl Extent {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for Extent {
    fn default() -> Self {
        Self::new()
    }
}

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
        Ok(return_field_impl(arg_fields, Dimension::XY)?)
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
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Extent3D;

impl Extent3D {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for Extent3D {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateUDFImpl for Extent3D {
    fn name(&self) -> &str {
        "st_3dextent"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        Ok(return_field_impl(arg_fields, Dimension::XYZ)?)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let input_field = acc_args.exprs[0].return_field(acc_args.schema)?;
        Ok(Box::new(Extent3DAccumulator::new(
            input_field,
            acc_args.return_field,
        )))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(["xmin", "ymin", "zmin", "xmax", "ymax", "zmax"]
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
}

fn return_field_impl(args: &[FieldRef], dimension: Dimension) -> GeoDataFusionResult<FieldRef> {
    let metadata = field_metadata(args[0].as_ref())?;
    let output_type = BoxType::new(dimension, metadata);
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
        if is_null_geometry_input(self.field.as_ref(), value.as_ref()) {
            return Ok(());
        }
        let geo_arr = geometry_array_from_arrow(value, &self.field)?;
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

#[derive(Debug)]
struct Extent3DAccumulator {
    field: FieldRef,
    return_field: FieldRef,
    bounds: Option<Extent3DBounds>,
}

impl Extent3DAccumulator {
    pub fn new(field: FieldRef, return_field: FieldRef) -> Self {
        Self {
            field,
            return_field,
            bounds: None,
        }
    }

    fn update_bounds(&mut self, bounds: Extent3DBounds) {
        self.bounds = Some(match self.bounds {
            Some(current) => current.merge(bounds),
            None => bounds,
        });
    }
}

impl Accumulator for Extent3DAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(match self.bounds {
            Some(bounds) => {
                let min = bounds.min();
                let max = bounds.max();
                vec![
                    min[0].into(),
                    min[1].into(),
                    min[2].into(),
                    max[0].into(),
                    max[1].into(),
                    max[2].into(),
                ]
            }
            None => vec![
                ScalarValue::Float64(None),
                ScalarValue::Float64(None),
                ScalarValue::Float64(None),
                ScalarValue::Float64(None),
                ScalarValue::Float64(None),
                ScalarValue::Float64(None),
            ],
        })
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let Some(bounds) = self.bounds else {
            return ScalarValue::try_from(self.return_field.data_type());
        };
        let DataType::Struct(fields) = self.return_field.data_type() else {
            return Err(DataFusionError::Internal(format!(
                "ST_3DExtent expected a struct return type, got {}",
                self.return_field.data_type()
            )));
        };
        let min = bounds.min();
        let max = bounds.max();

        ScalarStructBuilder::new()
            .with_scalar(Arc::clone(&fields[0]), min[0].into())
            .with_scalar(Arc::clone(&fields[1]), min[1].into())
            .with_scalar(Arc::clone(&fields[2]), min[2].into())
            .with_scalar(Arc::clone(&fields[3]), max[0].into())
            .with_scalar(Arc::clone(&fields[4]), max[1].into())
            .with_scalar(Arc::clone(&fields[5]), max[2].into())
            .build()
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let value = values.first().ok_or_else(|| {
            DataFusionError::Internal("ST_3DExtent received no input batch".to_string())
        })?;
        if is_null_geometry_input(self.field.as_ref(), value.as_ref()) {
            return Ok(());
        }
        let geo_arr = geometry_array_from_arrow(value, &self.field)?;
        if let Some(bounds) = total_bounds_3d(&geo_arr).map_err(GeoDataFusionError::GeoArrow)? {
            self.update_bounds(bounds);
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.len() != 6 {
            return Err(DataFusionError::Internal(format!(
                "ST_3DExtent expected six state arrays, got {}",
                states.len()
            )));
        }

        let state_array = |index: usize| -> Result<&arrow_array::PrimitiveArray<Float64Type>> {
            states[index]
                .as_primitive_opt::<Float64Type>()
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "ST_3DExtent state field {index} has type {}, expected Float64",
                        states[index].data_type()
                    ))
                })
        };
        let merged = [
            arrow_arith::aggregate::min(state_array(0)?),
            arrow_arith::aggregate::min(state_array(1)?),
            arrow_arith::aggregate::min(state_array(2)?),
            arrow_arith::aggregate::max(state_array(3)?),
            arrow_arith::aggregate::max(state_array(4)?),
            arrow_arith::aggregate::max(state_array(5)?),
        ];
        if let [
            Some(xmin),
            Some(ymin),
            Some(zmin),
            Some(xmax),
            Some(ymax),
            Some(zmax),
        ] = merged
            && [xmin, ymin, zmin, xmax, ymax, zmax]
                .into_iter()
                .all(f64::is_finite)
        {
            self.update_bounds(Extent3DBounds::new([xmin, ymin, zmin], [xmax, ymax, zmax]));
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

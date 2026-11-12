use std::sync::{Arc, OnceLock};

use arrow_array::new_null_array;
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::scalar::ScalarValue;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::cast::AsGeoArrowArray;
use geoarrow_schema::error::GeoArrowResult;
use geoarrow_schema::{CoordType, Dimension, GeoArrowType, GeometryType};

use crate::error::GeoDataFusionResult;
use crate::udf::native::editors::force_array_to_dimension;

/// `ST_Simplify` scalar UDF implementation: simplifies a geometry using the Douglas-Peucker algorithm.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Simplify {
    signature: Signature,
    coord_type: CoordType,
}

impl Simplify {
    /// Creates a new `ST_Simplify` UDF instance with the specified output [`CoordType`].
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            coord_type,
        }
    }
}

impl Default for Simplify {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Simplify {
    fn name(&self) -> &str {
        "st_simplify"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field_impl(args, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(simplify_impl(
            args,
            self.coord_type,
            geoarrow_expr_geo::simplify,
        )?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Computes a simplified representation of a geometry using the Douglas-Peucker algorithm. The simplification tolerance is a distance value, in the units of the input SRS. Simplification removes vertices which are within the tolerance distance of the simplified linework. The result may not be valid even if the input is.",
                "ST_Simplify(geometry, epsilon)",
            )
            .with_argument("geom", "geometry")
            .with_argument("tolerance", "float")
            .build()
        }))
    }
}

/// `ST_SimplifyVW` scalar UDF implementation: simplifies a geometry using the Visvalingam-Whyatt algorithm.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct SimplifyVW {
    signature: Signature,
    coord_type: CoordType,
}

impl SimplifyVW {
    /// Creates a new `ST_SimplifyVW` UDF instance with the specified output [`CoordType`].
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            coord_type,
        }
    }
}

impl Default for SimplifyVW {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static DOCUMENTATION_VW: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for SimplifyVW {
    fn name(&self) -> &str {
        "st_simplifyvw"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field_impl(args, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(simplify_impl(
            args,
            self.coord_type,
            geoarrow_expr_geo::simplify_vw,
        )?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION_VW.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a simplified representation of a geometry using the Visvalingam-Whyatt algorithm. The simplification tolerance is an area value, in the units of the input SRS. Simplification removes vertices which form \"corners\" with area less than the tolerance. The result may not be valid even if the input is.",
                "ST_SimplifyVW(geometry, epsilon)",
            )
            .with_argument("geom", "geometry")
            .with_argument("tolerance", "float")
            .build()
        }))
    }
}

/// `ST_SimplifyPreserveTopology` scalar UDF implementation: simplifies a geometry using the topology-preserving Visvalingam-Whyatt area-threshold algorithm.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct SimplifyPreserveTopology {
    signature: Signature,
    coord_type: CoordType,
}

impl SimplifyPreserveTopology {
    /// Creates a new `ST_SimplifyPreserveTopology` UDF instance with the specified output [`CoordType`].
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            coord_type,
        }
    }
}

impl Default for SimplifyPreserveTopology {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static DOCUMENTATION_TOPO: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for SimplifyPreserveTopology {
    fn name(&self) -> &str {
        "st_simplifypreservetopology"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field_impl(args, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(simplify_impl(
            args,
            self.coord_type,
            geoarrow_expr_geo::simplify_vw_preserve,
        )?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION_TOPO.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Computes a simplified representation of a geometry using the topology-preserving Visvalingam-Whyatt algorithm. The simplification tolerance is an area value, in the units of the input SRS. Simplification removes vertices which form corners with area less than the tolerance, as long as topology is preserved. The result will be valid and simple if the input is.",
                "ST_SimplifyPreserveTopology(geometry, epsilon)",
            )
            .with_argument("geom", "geometry")
            .with_argument("tolerance", "float")
            .build()
        }))
    }
}

fn return_field_impl(
    args: ReturnFieldArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<FieldRef> {
    if args.arg_fields[0].data_type() == &DataType::Null {
        return Ok(Arc::new(
            GeometryType::new(Default::default())
                .with_coord_type(coord_type)
                .to_field("", true),
        ));
    }
    let geo_type = GeoArrowType::from_arrow_field(args.arg_fields[0].as_ref())?;
    let output_type = simplify_output_type(geo_type, coord_type);
    Ok(Arc::new(output_type.to_field("", true)))
}

fn simplify_output_type(geo_type: GeoArrowType, coord_type: CoordType) -> GeoArrowType {
    use GeoArrowType::*;
    match geo_type {
        Point(_)
        | LineString(_)
        | Polygon(_)
        | MultiPoint(_)
        | MultiLineString(_)
        | MultiPolygon(_)
        | GeometryCollection(_) => geo_type
            .with_dimension(Dimension::XY)
            .with_coord_type(coord_type),
        _ => GeometryType::new(geo_type.metadata().clone())
            .with_coord_type(coord_type)
            .into(),
    }
}

fn simplify_impl(
    args: ScalarFunctionArgs,
    coord_type: CoordType,
    simplify_fn: impl Fn(&dyn GeoArrowArray, f64) -> GeoArrowResult<Arc<dyn GeoArrowArray>>,
) -> GeoDataFusionResult<ColumnarValue> {
    let target = GeoArrowType::from_arrow_field(&args.return_field)?;
    if args.arg_fields[0].data_type() == &DataType::Null {
        return Ok(null_geometry_array(&target, args.number_rows).into());
    }

    let arrays = ColumnarValue::values_to_arrays(&args.args[0..1])?;
    let input = from_arrow_array(&arrays[0], &args.arg_fields[0])?;

    let epsilon = args.args[1].cast_to(&DataType::Float64, None)?;
    let epsilon = match epsilon {
        ColumnarValue::Scalar(ScalarValue::Float64(Some(val))) => val,
        ColumnarValue::Scalar(ScalarValue::Float64(None)) => {
            return Ok(null_geometry_array(&target, args.number_rows).into());
        }
        ColumnarValue::Scalar(_) => {
            return Err(DataFusionError::Internal(
                "ST_Simplify tolerance must be castable to Float64".to_string(),
            )
            .into());
        }
        ColumnarValue::Array(_) => {
            return Err(DataFusionError::NotImplemented(
                "Vectorized epsilon not yet implemented".to_string(),
            )
            .into());
        }
    };

    let normalized = if input.data_type() == target {
        input
    } else {
        normalize_to_target(input.as_ref(), &args.arg_fields[0], &target)?
    };
    let result = simplify_fn(normalized.as_ref(), epsilon)?;
    let result = match &target {
        GeoArrowType::Geometry(_) => {
            Arc::new(result.as_geometry().clone().into_coord_type(coord_type))
                as Arc<dyn GeoArrowArray>
        }
        _ => result,
    };
    Ok(result.to_array_ref().into())
}

fn normalize_to_target(
    input: &dyn GeoArrowArray,
    input_field: &arrow_schema::Field,
    target: &GeoArrowType,
) -> GeoDataFusionResult<Arc<dyn GeoArrowArray>> {
    force_array_to_dimension(input, input_field, target.clone(), Dimension::XY, 0.0, 0.0)
}

fn null_geometry_array(target: &GeoArrowType, number_rows: usize) -> arrow_array::ArrayRef {
    new_null_array(&target.to_data_type(), number_rows)
}

#[cfg(test)]
mod test {
    use arrow_array::cast::AsArray;
    use datafusion::prelude::*;
    use geo::line_string;
    use geoarrow_array::GeoArrowArrayAccessor;
    use geoarrow_array::array::GeometryArray;
    use geoarrow_expr_geo::util::to_geo::geometry_to_geo;

    use super::*;
    use crate::udf::native::io::{AsText, GeomFromText};

    #[tokio::test]
    async fn test_simplify() {
        let ctx = SessionContext::new();

        ctx.register_udf(Simplify::default().into());
        ctx.register_udf(GeomFromText::default().into());

        let df = ctx.sql(
            "SELECT ST_Simplify(ST_GeomFromText('LINESTRING(0.0 0.0, 5.0 4.0, 11.0 5.5, 17.3 3.2, 27.8 0.1)'), 1.0);").await.unwrap();

        let batches = df.collect().await.unwrap();
        let batch = batches.first().unwrap();
        let column = batch.column(0);

        let geom_arr =
            GeometryArray::try_from((column.as_ref(), batch.schema_ref().field(0))).unwrap();
        let expected = line_string![
            (x: 0.0, y: 0.0),
            (x: 5.0, y: 4.0),
            (x: 11.0, y: 5.5),
            (x: 27.8, y: 0.1),
        ];
        let expected = geo::Geometry::LineString(expected);
        assert_eq!(
            geometry_to_geo(&geom_arr.value(0).unwrap()).unwrap(),
            expected
        );
    }

    #[tokio::test]
    async fn test_simplify_vw() {
        let ctx = SessionContext::new();

        ctx.register_udf(SimplifyVW::default().into());
        ctx.register_udf(GeomFromText::default().into());
        ctx.register_udf(AsText::default().into());

        let df = ctx.sql(
            "SELECT ST_AsText(ST_SimplifyVW(ST_GeomFromText('LINESTRING(5 2, 3 8, 6 20, 7 25, 10 10)'), 30));").await.unwrap();
        let batches = df.collect().await.unwrap();
        let batch = batches.first().unwrap();
        let column = batch.column(0);
        let val = column.as_string::<i32>().value(0);
        assert_eq!(val, "LINESTRING(5 2,7 25,10 10)");
    }
}

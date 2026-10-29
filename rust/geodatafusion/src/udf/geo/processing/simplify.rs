use std::sync::{Arc, OnceLock};

use arrow_array::Array;
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::scalar::ScalarValue;
use geo_traits::to_geo::{ToGeoLineString, ToGeoMultiLineString, ToGeoMultiPolygon, ToGeoPolygon};
use geoarrow_array::builder::{
    LineStringBuilder, MultiLineStringBuilder, MultiPolygonBuilder, PolygonBuilder,
};
use geoarrow_array::cast::AsGeoArrowArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::error::GeoArrowResult;
use geoarrow_schema::{CoordType, Dimension, GeoArrowType, GeometryType};

use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::{
    field_metadata_or_default, geometry_array_from_arrow, is_null_geometry_input,
};

#[derive(Debug, Clone, Copy)]
enum SimplifyKind {
    DouglasPeucker,
    VisvalingamWhyatt,
    VisvalingamWhyattPreserve,
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Simplify {
    signature: Signature,
    coord_type: CoordType,
}

impl Simplify {
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
        // TODO: pass down coord_type
        Ok(simplify_impl(args, SimplifyKind::DouglasPeucker)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Computes a simplified representation of a geometry using the Douglas-Peucker algorithm. The simplification tolerance is a distance value, in the units of the input SRS. Simplification removes vertices which are within the tolerance distance of the simplified linework. The result may not be valid even if the input is. Simplification runs in XY; LineString, Polygon, MultiLineString, and MultiPolygon results drop Z and M. Point, MultiPoint, and typed GeometryCollection inputs are returned unchanged.",
                "ST_Simplify(geometry, epsilon)",
            )
            .with_argument("geom", "geometry")
            .with_argument("tolerance", "float")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct SimplifyVW {
    signature: Signature,
    coord_type: CoordType,
}

impl SimplifyVW {
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
        // TODO: pass down coord_type
        Ok(simplify_impl(args, SimplifyKind::VisvalingamWhyatt)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION_VW.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a simplified representation of a geometry using the Visvalingam-Whyatt algorithm. The simplification tolerance is an area value, in the units of the input SRS. Simplification removes vertices which form \"corners\" with area less than the tolerance. The result may not be valid even if the input is. Simplification runs in XY; LineString, Polygon, MultiLineString, and MultiPolygon results drop Z and M. Point, MultiPoint, and typed GeometryCollection inputs are returned unchanged.",
                "ST_SimplifyVW(geometry, epsilon)",
            )
            .with_argument("geom", "geometry")
            .with_argument("tolerance", "float")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct SimplifyPreserveTopology {
    signature: Signature,
    coord_type: CoordType,
}

impl SimplifyPreserveTopology {
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
        // TODO: pass down coord_type
        Ok(simplify_impl(
            args,
            SimplifyKind::VisvalingamWhyattPreserve,
        )?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION_TOPO.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Computes a simplified representation of a geometry using a variant of the Douglas-Peucker algorithm which limits simplification to ensure the result has the same topology as the input. The simplification tolerance is a distance value, in the units of the input SRS. Simplification removes vertices which are within the tolerance distance of the simplified linework, as long as topology is preserved. The result will be valid and simple if the input is. Simplification runs in XY; LineString, Polygon, MultiLineString, and MultiPolygon results drop Z and M. Point, MultiPoint, and typed GeometryCollection inputs are returned unchanged.",
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
    let input_field = args.arg_fields[0].as_ref();
    let Ok(geo_type) = GeoArrowType::from_arrow_field(input_field) else {
        return Ok(Arc::new(
            GeometryType::new(field_metadata_or_default(input_field)?)
                .with_coord_type(coord_type)
                .to_field("", true),
        ));
    };

    let field = match geo_type {
        GeoArrowType::Point(_)
        | GeoArrowType::MultiPoint(_)
        | GeoArrowType::GeometryCollection(_) => geo_type.to_field("", true),
        GeoArrowType::LineString(typ) => typ.with_dimension(Dimension::XY).to_field("", true),
        GeoArrowType::MultiLineString(typ) => typ.with_dimension(Dimension::XY).to_field("", true),
        GeoArrowType::Polygon(typ) => typ.with_dimension(Dimension::XY).to_field("", true),
        GeoArrowType::MultiPolygon(typ) => typ.with_dimension(Dimension::XY).to_field("", true),
        _ => GeometryType::new(geo_type.metadata().clone())
            .with_coord_type(coord_type)
            .to_field("", true),
    };
    Ok(Arc::new(field))
}

fn simplify_impl(
    args: ScalarFunctionArgs,
    kind: SimplifyKind,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args[0..1])?;
    let input_field = args.arg_fields[0].as_ref();
    if is_null_geometry_input(input_field, arrays[0].as_ref()) {
        let output_type = GeoArrowType::from_arrow_field(args.return_field.as_ref())?;
        let array = crate::udf::native::transformation::geometry::null_array(
            &output_type,
            arrays[0].len(),
        )?;
        return Ok(ColumnarValue::Array(array.to_array_ref()));
    }

    let geo_array = geometry_array_from_arrow(&arrays[0], input_field)?;
    let epsilon = args.args[1].cast_to(&DataType::Float64, None)?;
    let epsilon = match epsilon {
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Float64(Some(val)) => val,
            ScalarValue::Float64(None) => {
                let output_type = GeoArrowType::from_arrow_field(args.return_field.as_ref())?;
                let array = crate::udf::native::transformation::geometry::null_array(
                    &output_type,
                    args.number_rows,
                )?;
                return Ok(ColumnarValue::Array(array.to_array_ref()));
            }
            _ => unreachable!(),
        },
        ColumnarValue::Array(_) => {
            return Err(DataFusionError::NotImplemented(
                "Vectorized epsilon not yet implemented".to_string(),
            )
            .into());
        }
    };

    let epsilon = match kind {
        SimplifyKind::DouglasPeucker => epsilon.abs(),
        SimplifyKind::VisvalingamWhyatt => epsilon,
        SimplifyKind::VisvalingamWhyattPreserve => {
            if epsilon < 0.0 {
                return Err(DataFusionError::Execution(
                    "ST_SimplifyPreserveTopology: Tolerance must be non-negative".to_string(),
                )
                .into());
            }
            epsilon
        }
    };

    let geo_array = normalize_simplify_input(geo_array)?;
    let simplify_fn = match kind {
        SimplifyKind::DouglasPeucker => geoarrow_expr_geo::simplify,
        SimplifyKind::VisvalingamWhyatt => geoarrow_expr_geo::simplify_vw,
        SimplifyKind::VisvalingamWhyattPreserve => geoarrow_expr_geo::simplify_vw_preserve,
    };
    let result = simplify_fn(geo_array.as_ref(), epsilon)?;
    Ok(result.to_array_ref().into())
}

/// Apply the declared XY contract before the upstream simplify implementation.
///
/// GeoArrow's simplify reuses the input extension type. For measured line and
/// polygon arrays that would build an XYZ/XYM/XYZM result while this UDF
/// declares an XY result. Rebuild only the affected typed families in XY so
/// the array and the return field agree, and Z/M are dropped consistently.
fn normalize_simplify_input(
    array: Arc<dyn GeoArrowArray>,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    match array.data_type() {
        GeoArrowType::LineString(typ) if typ.dimension() != Dimension::XY => {
            let mut builder = LineStringBuilder::new(typ.with_dimension(Dimension::XY));
            for item in array.as_line_string().iter() {
                if let Some(geometry) = item {
                    builder.push_line_string(Some(&geometry?.to_line_string()))?;
                } else {
                    builder.push_line_string(None::<&geo::LineString>.as_ref())?;
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        GeoArrowType::Polygon(typ) if typ.dimension() != Dimension::XY => {
            let mut builder = PolygonBuilder::new(typ.with_dimension(Dimension::XY));
            for item in array.as_polygon().iter() {
                if let Some(geometry) = item {
                    builder.push_polygon(Some(&geometry?.to_polygon()))?;
                } else {
                    builder.push_polygon(None::<&geo::Polygon>.as_ref())?;
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        GeoArrowType::MultiLineString(typ) if typ.dimension() != Dimension::XY => {
            let mut builder = MultiLineStringBuilder::new(typ.with_dimension(Dimension::XY));
            for item in array.as_multi_line_string().iter() {
                if let Some(geometry) = item {
                    builder.push_multi_line_string(Some(&geometry?.to_multi_line_string()))?;
                } else {
                    builder.push_multi_line_string(None::<&geo::MultiLineString>.as_ref())?;
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        GeoArrowType::MultiPolygon(typ) if typ.dimension() != Dimension::XY => {
            let mut builder = MultiPolygonBuilder::new(typ.with_dimension(Dimension::XY));
            for item in array.as_multi_polygon().iter() {
                if let Some(geometry) = item {
                    builder.push_multi_polygon(Some(&geometry?.to_multi_polygon()))?;
                } else {
                    builder.push_multi_polygon(None::<&geo::MultiPolygon>.as_ref())?;
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => Ok(array),
    }
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
        ctx.register_udf(AsText.into());

        let df = ctx.sql(
            "SELECT ST_AsText(ST_SimplifyVW(ST_GeomFromText('LINESTRING(5 2, 3 8, 6 20, 7 25, 10 10)'), 30));").await.unwrap();
        let batches = df.collect().await.unwrap();
        let batch = batches.first().unwrap();
        let column = batch.column(0);
        let val = column.as_string::<i32>().value(0);
        assert_eq!(val, "LINESTRING(5 2,7 25,10 10)");
    }
}

//! Advanced geometry processing functions: ST_LineMerge, ST_Snap,
//! ST_DelaunayTriangles, ST_VoronoiPolygons, ST_MinimumBoundingCircle,
//! ST_GeneratePoints, ST_ReducePrecision, ST_ChaikinSmoothing, ST_Segmentize.

use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::scalar::ScalarValue;
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::builder::GeometryBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_expr_geo::util::to_geo::geometry_to_geo;
use geoarrow_schema::error::GeoArrowResult;
use geoarrow_schema::{CoordType, GeometryType, Metadata};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;

// =========================================================================
// ST_ChaikinSmoothing
// =========================================================================
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct ChaikinSmoothing {
    signature: Signature,
    coord_type: CoordType,
}

impl ChaikinSmoothing {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            coord_type,
        }
    }
}

impl Default for ChaikinSmoothing {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static CHAIKIN_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for ChaikinSmoothing {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_chaikinsmoothing"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(geom_return_field(args, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(chaikin_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(CHAIKIN_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a smoothed version of a geometry using Chaikin's corner-cutting algorithm.",
                "ST_ChaikinSmoothing(geometry, nIterations)",
            )
            .with_argument("geom", "geometry")
            .with_argument("n_iterations", "integer - number of smoothing iterations")
            .build()
        }))
    }
}

fn chaikin_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args[0..1])?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;

    let n_iterations = match args.args[1].cast_to(&DataType::UInt32, None)? {
        ColumnarValue::Scalar(ScalarValue::UInt32(Some(n))) => n as usize,
        _ => 1,
    };

    let result = chaikin_array(&geo_array, n_iterations)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn chaikin_array(
    array: &dyn GeoArrowArray,
    n_iterations: usize,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(array, _chaikin_impl, n_iterations)
}

fn _chaikin_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    n_iterations: usize,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    use geo::ChaikinSmoothing as GeoChaikin;

    let geom_type = GeometryType::new(Default::default());
    let mut builder = GeometryBuilder::new(geom_type);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let smoothed = geo_geom.chaikin_smoothing(n_iterations);
            builder.push_geometry(Some(&smoothed))?;
        } else {
            builder.push_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

// =========================================================================
// ST_Segmentize
// =========================================================================
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Segmentize {
    signature: Signature,
    coord_type: CoordType,
}

impl Segmentize {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            coord_type,
        }
    }
}

impl Default for Segmentize {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static SEGMENTIZE_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Segmentize {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_segmentize"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(geom_return_field(args, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(segmentize_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(SEGMENTIZE_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a modified geometry with no segment longer than the given max_segment_length. Currently only supports LineString input. Other geometry types are returned unchanged.",
                "ST_Segmentize(geometry, max_segment_length)",
            )
            .with_argument("geom", "geometry")
            .with_argument("max_segment_length", "float8")
            .build()
        }))
    }
}

fn segmentize_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args[0..1])?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;

    let max_length = match args.args[1].cast_to(&DataType::Float64, None)? {
        ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => v,
        _ => {
            return Err(DataFusionError::NotImplemented(
                "Vectorized max_segment_length not supported".to_string(),
            )
            .into())
        }
    };

    let result = segmentize_array(&geo_array, max_length)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn segmentize_array(
    array: &dyn GeoArrowArray,
    max_length: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(array, _segmentize_impl, max_length)
}

fn _segmentize_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    max_length: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    use geo::LineStringSegmentize;

    let geom_type = GeometryType::new(Default::default());
    let mut builder = GeometryBuilder::new(geom_type);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let result = match geo_geom {
                geo::Geometry::LineString(ls) => {
                    let n_segments = (ls.euclidean_length() / max_length).ceil() as usize;
                    if n_segments > 1 {
                        if let Some(segmented) = ls.line_segmentize(n_segments) {
                            geo::Geometry::MultiLineString(segmented)
                        } else {
                            geo::Geometry::LineString(ls)
                        }
                    } else {
                        geo::Geometry::LineString(ls)
                    }
                }
                other => other,
            };
            builder.push_geometry(Some(&result))?;
        } else {
            builder.push_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

// =========================================================================
// ST_ReducePrecision
// =========================================================================
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct ReducePrecision {
    signature: Signature,
    coord_type: CoordType,
}

impl ReducePrecision {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            coord_type,
        }
    }
}

impl Default for ReducePrecision {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static REDUCE_PREC_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for ReducePrecision {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_reduceprecision"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(geom_return_field(args, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(reduce_precision_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(REDUCE_PREC_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a valid geometry with all points rounded to the given grid size.",
                "ST_ReducePrecision(geometry, gridSize)",
            )
            .with_argument("geom", "geometry")
            .with_argument("grid_size", "float8 - grid size for rounding coordinates")
            .build()
        }))
    }
}

fn reduce_precision_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args[0..1])?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;

    let grid_size = match args.args[1].cast_to(&DataType::Float64, None)? {
        ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => v,
        _ => {
            return Err(DataFusionError::NotImplemented(
                "Vectorized grid_size not supported".to_string(),
            )
            .into())
        }
    };

    let result = reduce_precision_array(&geo_array, grid_size)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn reduce_precision_array(
    array: &dyn GeoArrowArray,
    grid_size: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(array, _reduce_precision_impl, grid_size)
}

fn _reduce_precision_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    grid_size: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    use geo::MapCoords;

    let geom_type = GeometryType::new(Default::default());
    let mut builder = GeometryBuilder::new(geom_type);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let snapped = if grid_size > 0.0 {
                geo_geom.map_coords(|coord| {
                    geo::Coord {
                        x: (coord.x / grid_size).round() * grid_size,
                        y: (coord.y / grid_size).round() * grid_size,
                    }
                })
            } else {
                geo_geom
            };
            builder.push_geometry(Some(&snapped))?;
        } else {
            builder.push_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

// =========================================================================
// ST_UnaryUnion
// =========================================================================
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct UnaryUnion {
    coord_type: CoordType,
}

impl UnaryUnion {
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for UnaryUnion {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static UNARY_UNION_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for UnaryUnion {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_unaryunion"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(geom_return_field(args, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(unary_union_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(UNARY_UNION_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Computes the union of the components of a single geometry (useful for dissolving MultiPolygon or GeometryCollection into a single geometry).",
                "ST_UnaryUnion(geometry)",
            )
            .with_argument("geom", "geometry")
            .build()
        }))
    }
}

fn unary_union_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let result = unary_union_array(&geo_array)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn unary_union_array(array: &dyn GeoArrowArray) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(array, _unary_union_impl)
}

fn _unary_union_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    use geo::BooleanOps;

    let geom_type = GeometryType::new(Default::default());
    let mut builder = GeometryBuilder::new(geom_type);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let result = match geo_geom {
                geo::Geometry::MultiPolygon(mp) => {
                    let mut result = geo::MultiPolygon(vec![]);
                    for poly in mp.0 {
                        let mp_single = geo::MultiPolygon(vec![poly]);
                        result = result.union(&mp_single);
                    }
                    geo::Geometry::MultiPolygon(result)
                }
                geo::Geometry::GeometryCollection(gc) => {
                    let mut result = geo::MultiPolygon(vec![]);
                    for g in gc.0 {
                        match g {
                            geo::Geometry::Polygon(p) => {
                                let mp_single = geo::MultiPolygon(vec![p]);
                                result = result.union(&mp_single);
                            }
                            geo::Geometry::MultiPolygon(mp) => {
                                result = result.union(&mp);
                            }
                            other => {
                                let type_name = match &other {
                                    geo::Geometry::Point(_) => "Point",
                                    geo::Geometry::MultiPoint(_) => "MultiPoint",
                                    geo::Geometry::LineString(_) => "LineString",
                                    geo::Geometry::MultiLineString(_) => "MultiLineString",
                                    geo::Geometry::GeometryCollection(_) => "GeometryCollection",
                                    _ => "unsupported",
                                };
                                return Err(geoarrow_schema::error::GeoArrowError::IncorrectGeometryType(
                                    format!(
                                        "ST_UnaryUnion only supports polygonal geometries; found {type_name} in collection"
                                    ),
                                ));
                            }
                        }
                    }
                    geo::Geometry::MultiPolygon(result)
                }
                other => other,
            };
            builder.push_geometry(Some(&result))?;
        } else {
            builder.push_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

// =========================================================================
// Helper function for geometry return field
// =========================================================================
fn geom_return_field(
    args: ReturnFieldArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<FieldRef> {
    let metadata = Arc::new(Metadata::try_from(args.arg_fields[0].as_ref()).unwrap_or_default());
    let output_type = GeometryType::new(metadata).with_coord_type(coord_type);
    Ok(Arc::new(output_type.to_field("", true)))
}

// Need this for Segmentize
use geo::EuclideanLength;

#[cfg(test)]
mod test {
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::udf::native::io::{AsText, GeomFromText};

    #[tokio::test]
    async fn test_chaikin_smoothing() {
        let ctx = SessionContext::new();
        ctx.register_udf(ChaikinSmoothing::default().into());
        ctx.register_udf(GeomFromText::default().into());
        ctx.register_udf(AsText.into());

        let df = ctx
            .sql("SELECT ST_AsText(ST_ChaikinSmoothing(ST_GeomFromText('LINESTRING(0 0, 5 5, 10 0)'), 1));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[tokio::test]
    async fn test_reduce_precision() {
        let ctx = SessionContext::new();
        ctx.register_udf(ReducePrecision::default().into());
        ctx.register_udf(GeomFromText::default().into());
        ctx.register_udf(AsText.into());

        let df = ctx
            .sql(
                "SELECT ST_AsText(ST_ReducePrecision(ST_GeomFromText('POINT(1.23456 2.34567)'), 0.01));",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(val, "POINT(1.23 2.35)");
    }
}

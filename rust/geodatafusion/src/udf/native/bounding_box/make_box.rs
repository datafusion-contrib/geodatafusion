use std::sync::{Arc, OnceLock};

use arrow_buffer::{NullBuffer, NullBufferBuilder, ScalarBuffer};
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo_traits::{CoordTrait, GeometryTrait, GeometryType, PointTrait};
use geoarrow_array::array::{RectArray, SeparatedCoordBuffer, from_arrow_array};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::{BoxType, Dimension, GeoArrowType, Metadata};

use crate::error::GeoDataFusionResult;

/// `ST_MakeBox2D` scalar UDF implementation: creates a 2D bounding box (`BOX2D`) defined by two 2D point geometries.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct MakeBox2D;

impl MakeBox2D {
    /// Creates a new `ST_MakeBox2D` UDF instance.
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for MakeBox2D {
    fn default() -> Self {
        Self::new()
    }
}

static DOC_2D: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for MakeBox2D {
    fn name(&self) -> &str {
        "st_makebox2d"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: OnceLock<Signature> = OnceLock::new();
        SIGNATURE.get_or_init(|| Signature::any(2, datafusion::logical_expr::Volatility::Immutable))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        return_field_impl(args, Dimension::XY)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if is_null_input(&args) {
            return Ok(null_rect_array(&args, Dimension::XY)?
                .into_array_ref()
                .into());
        }
        Ok(make_box_impl(args, Dimension::XY)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOC_2D.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Creates a box2d defined by two Point geometries. This is useful for doing range queries.",
                "ST_MakeBox2D(ST_Point(-989502.1875, 528439.5625), ST_Point(-987121.375, 529933.1875))",
            )
            .with_argument("pointLowLeft", "geometry")
            .with_argument("pointUpRight", "geometry")
            .build()
        }))
    }
}

/// `ST_3DMakeBox` scalar UDF implementation: creates a 3D bounding box (`BOX3D`) defined by two 3D point geometries.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct MakeBox3D;

impl MakeBox3D {
    /// Creates a new `ST_3DMakeBox` UDF instance.
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for MakeBox3D {
    fn default() -> Self {
        Self::new()
    }
}

static DOC_3D: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for MakeBox3D {
    fn name(&self) -> &str {
        "st_3dmakebox"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: OnceLock<Signature> = OnceLock::new();
        SIGNATURE.get_or_init(|| Signature::any(2, datafusion::logical_expr::Volatility::Immutable))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        return_field_impl(args, Dimension::XYZ)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if is_null_input(&args) {
            return Ok(null_rect_array(&args, Dimension::XYZ)?
                .into_array_ref()
                .into());
        }
        Ok(make_box_impl(args, Dimension::XYZ)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOC_3D.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Creates a box3d defined by two 3D Point geometries.",
                "ST_3DMakeBox(ST_MakePoint(-989502.1875, 528439.5625, 10), ST_MakePoint(-987121.375, 529933.1875, 10))",
            )
            .with_argument("pointLowLeft", "geometry")
            .with_argument("pointUpRight", "geometry")
            .build()
        }))
    }
}

fn return_field_impl(args: ReturnFieldArgs, dimension: Dimension) -> Result<FieldRef> {
    let metadata = merged_metadata(args.arg_fields[0].as_ref(), args.arg_fields[1].as_ref())?;
    Ok(Arc::new(
        BoxType::new(dimension, metadata).to_field("", true),
    ))
}

fn merged_metadata(
    lower: &arrow_schema::Field,
    upper: &arrow_schema::Field,
) -> GeoDataFusionResult<Arc<Metadata>> {
    let lower = field_metadata(lower)?;
    let upper = field_metadata(upper)?;
    match (lower, upper) {
        (None, None) => Ok(Arc::new(Metadata::default())),
        (Some(metadata), None) | (None, Some(metadata)) => Ok(metadata),
        (Some(lower), Some(upper)) => {
            if lower.crs() != upper.crs() {
                return Err(DataFusionError::Execution(
                    "ST_MakeBox2D and ST_3DMakeBox require matching CRS metadata on both points"
                        .to_string(),
                )
                .into());
            }
            Ok(lower)
        }
    }
}

fn field_metadata(field: &arrow_schema::Field) -> GeoDataFusionResult<Option<Arc<Metadata>>> {
    if field.data_type() == &DataType::Null {
        Ok(None)
    } else {
        Ok(Some(
            GeoArrowType::from_arrow_field(field)?.metadata().clone(),
        ))
    }
}

fn is_null_input(args: &ScalarFunctionArgs) -> bool {
    args.arg_fields
        .iter()
        .take(2)
        .any(|field| field.data_type() == &DataType::Null)
        || args.args.iter().take(2).any(|arg| {
            matches!(
                arg,
                ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Null)
            )
        })
}

fn null_rect_array(
    args: &ScalarFunctionArgs,
    dimension: Dimension,
) -> GeoDataFusionResult<RectArray> {
    let metadata = args
        .return_field
        .extension_type::<BoxType>()
        .metadata()
        .clone();
    let rows = args.number_rows;
    let lower = SeparatedCoordBuffer::from_vec(
        (0..dimension.size())
            .map(|_| ScalarBuffer::from(vec![f64::NAN; rows]))
            .collect(),
        dimension,
    )?;
    let upper = SeparatedCoordBuffer::from_vec(
        (0..dimension.size())
            .map(|_| ScalarBuffer::from(vec![f64::NAN; rows]))
            .collect(),
        dimension,
    )?;
    Ok(RectArray::new(
        lower,
        upper,
        Some(NullBuffer::from(vec![false; rows])),
        metadata,
    ))
}

fn make_box_impl(
    args: ScalarFunctionArgs,
    dimension: Dimension,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let lower = from_arrow_array(&arrays[0], &args.arg_fields[0]).map_err(|err| {
        DataFusionError::Execution(format!(
            "failed to decode lower point: field={:?}, array={:?}, error={err}",
            args.arg_fields[0],
            arrays[0].data_type()
        ))
    })?;
    let upper = from_arrow_array(&arrays[1], &args.arg_fields[1]).map_err(|err| {
        DataFusionError::Execution(format!(
            "failed to decode upper point: field={:?}, array={:?}, error={err}",
            args.arg_fields[1],
            arrays[1].data_type()
        ))
    })?;
    let lower_ref = lower.as_ref();
    let upper_ref = upper.as_ref();
    let lower_points = downcast_geoarrow_array!(lower_ref, extract_points, dimension)?;
    let upper_points = downcast_geoarrow_array!(upper_ref, extract_points, dimension)?;

    let metadata = merged_metadata(args.arg_fields[0].as_ref(), args.arg_fields[1].as_ref())?;

    let mut lower_values = vec![Vec::with_capacity(arrays[0].len()); dimension.size()];
    let mut upper_values = vec![Vec::with_capacity(arrays[0].len()); dimension.size()];
    let mut nulls = NullBufferBuilder::new(arrays[0].len());

    for (lower, upper) in lower_points.into_iter().zip(upper_points) {
        match (lower, upper) {
            (Some(lower), Some(upper)) => {
                for index in 0..dimension.size() {
                    lower_values[index].push(lower[index]);
                    upper_values[index].push(upper[index]);
                }
                nulls.append_non_null();
            }
            _ => {
                for index in 0..dimension.size() {
                    lower_values[index].push(f64::NAN);
                    upper_values[index].push(f64::NAN);
                }
                nulls.append_null();
            }
        }
    }

    let lower = SeparatedCoordBuffer::from_vec(
        lower_values.into_iter().map(ScalarBuffer::from).collect(),
        dimension,
    )?;
    let upper = SeparatedCoordBuffer::from_vec(
        upper_values.into_iter().map(ScalarBuffer::from).collect(),
        dimension,
    )?;
    let rect_arr = RectArray::new(lower, upper, nulls.build(), metadata);

    Ok(rect_arr.into_array_ref().into())
}

fn extract_points<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    dimension: Dimension,
) -> GeoDataFusionResult<Vec<Option<[f64; 3]>>> {
    let mut points = Vec::with_capacity(array.len());
    for geom in array.iter() {
        let Some(geom) = geom else {
            points.push(None);
            continue;
        };
        let geom = geom?;
        let point = match geom.as_type() {
            GeometryType::Point(point) => point,
            other => {
                return Err(DataFusionError::Execution(format!(
                    "ST_MakeBox2D and ST_3DMakeBox require Point inputs, got {}",
                    geometry_type_name(other)
                ))
                .into());
            }
        };
        let Some(coord) = point.coord() else {
            points.push(None);
            continue;
        };
        let z = if dimension == Dimension::XYZ {
            if !matches!(
                coord.dim(),
                geo_traits::Dimensions::Xyz | geo_traits::Dimensions::Xyzm
            ) {
                return Err(DataFusionError::Execution(
                    "ST_3DMakeBox requires points with a Z coordinate".to_string(),
                )
                .into());
            }
            coord.nth(2).unwrap_or(f64::NAN)
        } else {
            0.0
        };
        points.push(Some([coord.x(), coord.y(), z]));
    }
    Ok(points)
}

fn geometry_type_name<'a, P, LS, Y, MP, ML, MY, GC, R, T, L>(
    geometry: GeometryType<'a, P, LS, Y, MP, ML, MY, GC, R, T, L>,
) -> &'static str
where
    P: geo_traits::PointTrait,
    LS: geo_traits::LineStringTrait,
    Y: geo_traits::PolygonTrait,
    MP: geo_traits::MultiPointTrait,
    ML: geo_traits::MultiLineStringTrait,
    MY: geo_traits::MultiPolygonTrait,
    GC: geo_traits::GeometryCollectionTrait,
    R: geo_traits::RectTrait,
    T: geo_traits::TriangleTrait,
    L: geo_traits::LineTrait,
{
    match geometry {
        GeometryType::Point(_) => "POINT",
        GeometryType::LineString(_) => "LINESTRING",
        GeometryType::Polygon(_) => "POLYGON",
        GeometryType::MultiPoint(_) => "MULTIPOINT",
        GeometryType::MultiLineString(_) => "MULTILINESTRING",
        GeometryType::MultiPolygon(_) => "MULTIPOLYGON",
        GeometryType::GeometryCollection(_) => "GEOMETRYCOLLECTION",
        GeometryType::Rect(_) => "RECT",
        GeometryType::Triangle(_) => "TRIANGLE",
        GeometryType::Line(_) => "LINE",
    }
}

#[cfg(test)]
mod test {
    use approx::relative_eq;
    use datafusion::prelude::*;
    use geo_traits::{CoordTrait, RectTrait};
    use geoarrow_array::GeoArrowArrayAccessor;

    use super::*;
    use crate::udf::native::constructors::{Point, PointZ};

    #[tokio::test]
    async fn test_2d() {
        let ctx = SessionContext::new();

        ctx.register_udf(MakeBox2D::new().into());
        ctx.register_udf(Point::default().into());

        let out = ctx
            .sql("SELECT ST_MakeBox2D(ST_Point(0, 5), ST_Point(10, 20));")
            .await
            .unwrap();
        let batch = out.collect().await.unwrap().into_iter().next().unwrap();
        let schema = batch.schema();
        let rect_array =
            RectArray::try_from((batch.columns()[0].as_ref(), schema.field(0))).unwrap();
        let rect = rect_array.value(0).unwrap();

        assert!(relative_eq!(rect.min().x(), 0.0));
        assert!(relative_eq!(rect.min().y(), 5.0));
        assert!(relative_eq!(rect.max().x(), 10.0));
        assert!(relative_eq!(rect.max().y(), 20.0));
    }

    #[tokio::test]
    async fn test_3d() {
        let ctx = SessionContext::new();

        ctx.register_udf(MakeBox3D::new().into());
        ctx.register_udf(PointZ::default().into());

        let out = ctx
            .sql("SELECT ST_3DMakeBox(ST_PointZ(0, 5, 1), ST_PointZ(10, 20, 30));")
            .await
            .unwrap();
        let batch = out.collect().await.unwrap().into_iter().next().unwrap();
        let schema = batch.schema();
        let rect_array =
            RectArray::try_from((batch.columns()[0].as_ref(), schema.field(0))).unwrap();
        let rect = rect_array.value(0).unwrap();

        assert!(relative_eq!(rect.min().x(), 0.0));
        assert!(relative_eq!(rect.min().y(), 5.0));
        assert!(relative_eq!(rect.min().nth_or_panic(2), 1.0));
        assert!(relative_eq!(rect.max().x(), 10.0));
        assert!(relative_eq!(rect.max().y(), 20.0));
        assert!(relative_eq!(rect.max().nth_or_panic(2), 30.0));
    }
}

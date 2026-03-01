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
use geo::{AffineOps, AffineTransform, Rotate, Scale, Translate};
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::builder::GeometryBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_expr_geo::util::to_geo::geometry_to_geo;
use geoarrow_schema::error::GeoArrowResult;
use geoarrow_schema::{CoordType, GeometryType, Metadata};

use crate::error::GeoDataFusionResult;

// ---------------------------------------------------------------------------
// ST_Translate
// ---------------------------------------------------------------------------

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct StTranslate {
    signature: Signature,
    coord_type: CoordType,
}

impl StTranslate {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
            coord_type,
        }
    }
}

impl Default for StTranslate {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static TRANSLATE_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for StTranslate {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_translate"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(geometry_return_field(args, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(translate_impl(args, self.coord_type)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(TRANSLATE_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Translates a geometry by the given offsets.",
                "ST_Translate(geometry, deltaX, deltaY)",
            )
            .with_argument("geom", "geometry")
            .with_argument("deltaX", "float8 - offset to add to X coordinates")
            .with_argument("deltaY", "float8 - offset to add to Y coordinates")
            .build()
        }))
    }
}

fn translate_impl(
    args: ScalarFunctionArgs,
    _coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args[0..1])?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;

    let delta_x = match args.args[1].cast_to(&DataType::Float64, None)? {
        ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => v,
        _ => {
            return Err(DataFusionError::NotImplemented(
                "Vectorized deltaX not yet implemented".to_string(),
            )
            .into());
        }
    };

    let delta_y = match args.args[2].cast_to(&DataType::Float64, None)? {
        ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => v,
        _ => {
            return Err(DataFusionError::NotImplemented(
                "Vectorized deltaY not yet implemented".to_string(),
            )
            .into());
        }
    };

    let result = translate_array(&geo_array, delta_x, delta_y)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn translate_array(
    array: &dyn GeoArrowArray,
    delta_x: f64,
    delta_y: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(array, _translate_impl, delta_x, delta_y)
}

fn _translate_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    delta_x: f64,
    delta_y: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let geom_type = GeometryType::new(Default::default());
    let mut builder = GeometryBuilder::new(geom_type);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let translated = geo_geom.translate(delta_x, delta_y);
            builder.push_geometry(Some(&translated))?;
        } else {
            builder.push_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

// ---------------------------------------------------------------------------
// ST_Scale
// ---------------------------------------------------------------------------

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct StScale {
    signature: Signature,
    coord_type: CoordType,
}

impl StScale {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
            coord_type,
        }
    }
}

impl Default for StScale {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static SCALE_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for StScale {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_scale"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(geometry_return_field(args, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(scale_impl(args, self.coord_type)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(SCALE_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Scales a geometry by the given factors.",
                "ST_Scale(geometry, xFactor, yFactor)",
            )
            .with_argument("geom", "geometry")
            .with_argument("xFactor", "float8 - factor to scale X coordinates by")
            .with_argument("yFactor", "float8 - factor to scale Y coordinates by")
            .build()
        }))
    }
}

fn scale_impl(
    args: ScalarFunctionArgs,
    _coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args[0..1])?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;

    let x_factor = match args.args[1].cast_to(&DataType::Float64, None)? {
        ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => v,
        _ => {
            return Err(DataFusionError::NotImplemented(
                "Vectorized xFactor not yet implemented".to_string(),
            )
            .into());
        }
    };

    let y_factor = match args.args[2].cast_to(&DataType::Float64, None)? {
        ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => v,
        _ => {
            return Err(DataFusionError::NotImplemented(
                "Vectorized yFactor not yet implemented".to_string(),
            )
            .into());
        }
    };

    let result = scale_array(&geo_array, x_factor, y_factor)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn scale_array(
    array: &dyn GeoArrowArray,
    x_factor: f64,
    y_factor: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(array, _scale_impl, x_factor, y_factor)
}

fn _scale_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    x_factor: f64,
    y_factor: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let geom_type = GeometryType::new(Default::default());
    let mut builder = GeometryBuilder::new(geom_type);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let scaled = geo_geom.scale_xy(x_factor, y_factor);
            builder.push_geometry(Some(&scaled))?;
        } else {
            builder.push_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

// ---------------------------------------------------------------------------
// ST_Rotate
// ---------------------------------------------------------------------------

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct StRotate {
    signature: Signature,
    coord_type: CoordType,
}

impl StRotate {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            coord_type,
        }
    }
}

impl Default for StRotate {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static ROTATE_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for StRotate {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_rotate"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(geometry_return_field(args, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(rotate_impl(args, self.coord_type)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(ROTATE_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Rotates a geometry around its centroid by the given angle in radians.",
                "ST_Rotate(geometry, angle)",
            )
            .with_argument("geom", "geometry")
            .with_argument("angle", "float8 - rotation angle in radians")
            .build()
        }))
    }
}

fn rotate_impl(
    args: ScalarFunctionArgs,
    _coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args[0..1])?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;

    let angle = match args.args[1].cast_to(&DataType::Float64, None)? {
        ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => v,
        _ => {
            return Err(DataFusionError::NotImplemented(
                "Vectorized angle not yet implemented".to_string(),
            )
            .into());
        }
    };

    // Convert radians to degrees for geo::Rotate which expects degrees
    let angle_degrees = angle.to_degrees();

    let result = rotate_array(&geo_array, angle_degrees)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn rotate_array(
    array: &dyn GeoArrowArray,
    angle_degrees: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(array, _rotate_impl, angle_degrees)
}

fn _rotate_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    angle_degrees: f64,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let geom_type = GeometryType::new(Default::default());
    let mut builder = GeometryBuilder::new(geom_type);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let rotated = geo_geom.rotate_around_centroid(angle_degrees);
            builder.push_geometry(Some(&rotated))?;
        } else {
            builder.push_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

// ---------------------------------------------------------------------------
// ST_Affine
// ---------------------------------------------------------------------------

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct StAffine {
    signature: Signature,
    coord_type: CoordType,
}

impl StAffine {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::any(7, Volatility::Immutable),
            coord_type,
        }
    }
}

impl Default for StAffine {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static AFFINE_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for StAffine {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_affine"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(geometry_return_field(args, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(affine_impl(args, self.coord_type)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(AFFINE_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Applies a 2D affine transformation to a geometry. The transformation matrix is: | a  b  xoff | | d  e  yoff | | 0  0  1    |",
                "ST_Affine(geometry, a, b, d, e, xoff, yoff)",
            )
            .with_argument("geom", "geometry")
            .with_argument("a", "float8 - coefficient a of the affine transform")
            .with_argument("b", "float8 - coefficient b of the affine transform")
            .with_argument("d", "float8 - coefficient d of the affine transform")
            .with_argument("e", "float8 - coefficient e of the affine transform")
            .with_argument("xoff", "float8 - x translation offset")
            .with_argument("yoff", "float8 - y translation offset")
            .build()
        }))
    }
}

fn extract_scalar_f64(col: &ColumnarValue, param_name: &str) -> GeoDataFusionResult<f64> {
    match col.cast_to(&DataType::Float64, None)? {
        ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => Ok(v),
        _ => Err(DataFusionError::NotImplemented(format!(
            "Vectorized {} not yet implemented",
            param_name
        ))
        .into()),
    }
}

fn affine_impl(
    args: ScalarFunctionArgs,
    _coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args[0..1])?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;

    let a = extract_scalar_f64(&args.args[1], "a")?;
    let b = extract_scalar_f64(&args.args[2], "b")?;
    let d = extract_scalar_f64(&args.args[3], "d")?;
    let e = extract_scalar_f64(&args.args[4], "e")?;
    let xoff = extract_scalar_f64(&args.args[5], "xoff")?;
    let yoff = extract_scalar_f64(&args.args[6], "yoff")?;

    let transform = AffineTransform::new(a, b, xoff, d, e, yoff);

    let result = affine_array(&geo_array, &transform)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn affine_array(
    array: &dyn GeoArrowArray,
    transform: &AffineTransform,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(array, _affine_impl, transform)
}

fn _affine_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    transform: &AffineTransform,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let geom_type = GeometryType::new(Default::default());
    let mut builder = GeometryBuilder::new(geom_type);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let transformed = geo_geom.affine_transform(transform);
            builder.push_geometry(Some(&transformed))?;
        } else {
            builder.push_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn geometry_return_field(
    args: ReturnFieldArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<FieldRef> {
    let metadata = Arc::new(Metadata::try_from(args.arg_fields[0].as_ref()).unwrap_or_default());
    let output_type = GeometryType::new(metadata).with_coord_type(coord_type);
    Ok(Arc::new(output_type.to_field("", true)))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod test {
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::udf::native::io::{AsText, GeomFromText};

    #[tokio::test]
    async fn test_translate() {
        let ctx = SessionContext::new();
        ctx.register_udf(StTranslate::default().into());
        ctx.register_udf(AsText.into());
        ctx.register_udf(GeomFromText::default().into());

        let df = ctx
            .sql("SELECT ST_AsText(ST_Translate(ST_GeomFromText('POINT(0 0)'), 1, 2));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(val, "POINT(1 2)");
    }

    #[tokio::test]
    async fn test_scale() {
        let ctx = SessionContext::new();
        ctx.register_udf(StScale::default().into());
        ctx.register_udf(AsText.into());
        ctx.register_udf(GeomFromText::default().into());

        // ST_Scale scales around the bounding box center (centroid).
        // LINESTRING(0 0, 2 0) has bounding rect center at (1, 0).
        // Scaling by (xFactor=2, yFactor=3) around (1, 0):
        //   (0,0) -> (1 + 2*(0-1), 0 + 3*(0-0)) = (-1, 0)
        //   (2,0) -> (1 + 2*(2-1), 0 + 3*(0-0)) = (3, 0)
        let df = ctx
            .sql("SELECT ST_AsText(ST_Scale(ST_GeomFromText('LINESTRING(0 0, 2 0)'), 2, 3));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(val, "LINESTRING(-1 0,3 0)");
    }

    #[tokio::test]
    async fn test_rotate() {
        let ctx = SessionContext::new();
        ctx.register_udf(StRotate::default().into());
        ctx.register_udf(GeomFromText::default().into());

        // ST_Rotate rotates around the geometry centroid by an angle in radians.
        // LINESTRING(-1 0, 1 0) has centroid at (0, 0).
        // Rotating 90 degrees CCW (PI/2 radians) around (0, 0): (x,y) -> (-y, x).
        //   (-1, 0) -> (0, -1)
        //   (1, 0)  -> (0, 1)
        let df = ctx
            .sql(&format!(
                "SELECT ST_Rotate(ST_GeomFromText('LINESTRING(-1 0, 1 0)'), {});",
                std::f64::consts::FRAC_PI_2
            ))
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let column = batch.column(0);

        let geom_arr = geoarrow_array::array::GeometryArray::try_from((
            column.as_ref(),
            batch.schema_ref().field(0),
        ))
        .unwrap();
        let geo_geom = geometry_to_geo(&geom_arr.value(0).unwrap()).unwrap();
        if let geo::Geometry::LineString(ls) = geo_geom {
            let coords: Vec<_> = ls.coords().collect();
            assert_eq!(coords.len(), 2);
            // First point: (-1, 0) rotated 90 CCW -> (0, -1)
            assert!(
                (coords[0].x - 0.0).abs() < 1e-10,
                "Expected x[0] ~ 0, got {}",
                coords[0].x
            );
            assert!(
                (coords[0].y - (-1.0)).abs() < 1e-10,
                "Expected y[0] ~ -1, got {}",
                coords[0].y
            );
            // Second point: (1, 0) rotated 90 CCW -> (0, 1)
            assert!(
                (coords[1].x - 0.0).abs() < 1e-10,
                "Expected x[1] ~ 0, got {}",
                coords[1].x
            );
            assert!(
                (coords[1].y - 1.0).abs() < 1e-10,
                "Expected y[1] ~ 1, got {}",
                coords[1].y
            );
        } else {
            panic!("Expected LineString geometry");
        }
    }

    #[tokio::test]
    async fn test_affine_translate() {
        let ctx = SessionContext::new();
        ctx.register_udf(StAffine::default().into());
        ctx.register_udf(AsText.into());
        ctx.register_udf(GeomFromText::default().into());

        // Identity rotation with translation (1, 2): a=1, b=0, d=0, e=1, xoff=1, yoff=2
        let df = ctx
            .sql(
                "SELECT ST_AsText(ST_Affine(ST_GeomFromText('POINT(0 0)'), 1, 0, 0, 1, 1, 2));",
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
        assert_eq!(val, "POINT(1 2)");
    }

    #[tokio::test]
    async fn test_affine_scale() {
        let ctx = SessionContext::new();
        ctx.register_udf(StAffine::default().into());
        ctx.register_udf(AsText.into());
        ctx.register_udf(GeomFromText::default().into());

        // Scale by (3, 2) with no translation: a=3, b=0, d=0, e=2, xoff=0, yoff=0
        let df = ctx
            .sql(
                "SELECT ST_AsText(ST_Affine(ST_GeomFromText('POINT(1 1)'), 3, 0, 0, 2, 0, 0));",
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
        assert_eq!(val, "POINT(3 2)");
    }
}

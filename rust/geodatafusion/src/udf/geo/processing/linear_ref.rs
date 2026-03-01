use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow_array::builder::Float64Builder;
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::scalar::ScalarValue;
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::builder::{GeometryBuilder, PointBuilder};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_expr_geo::util::to_geo::geometry_to_geo;
use geoarrow_schema::error::GeoArrowResult;
use geoarrow_schema::{CoordType, Dimension, GeometryType, Metadata, PointType};

use crate::error::GeoDataFusionResult;

// ---------------------------------------------------------------------------
// ST_LineInterpolatePoint
// ---------------------------------------------------------------------------

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct LineInterpolatePoint {
    signature: Signature,
    coord_type: CoordType,
}

impl LineInterpolatePoint {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            coord_type,
        }
    }
}

impl Default for LineInterpolatePoint {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static LIP_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for LineInterpolatePoint {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_lineinterpolatepoint"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let metadata =
            Arc::new(Metadata::try_from(args.arg_fields[0].as_ref()).unwrap_or_default());
        let output_type =
            PointType::new(Dimension::XY, metadata).with_coord_type(self.coord_type);
        Ok(Arc::new(output_type.to_field("", true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(line_interpolate_impl(args, self.coord_type)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(LIP_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a point interpolated along a line at a fractional location. The first argument must be a LineString. The second argument is a float between 0 and 1 representing the fraction of the total line length where the point is located.",
                "ST_LineInterpolatePoint(linestring, fraction)",
            )
            .with_argument("geom", "geometry (LineString)")
            .with_argument("fraction", "float8 - value between 0 and 1")
            .build()
        }))
    }
}

fn line_interpolate_impl(
    args: ScalarFunctionArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args[0..1])?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;

    let fraction = match args.args[1].cast_to(&DataType::Float64, None)? {
        ColumnarValue::Scalar(ScalarValue::Float64(Some(f))) => f,
        _ => {
            return Err(DataFusionError::NotImplemented(
                "Vectorized fraction not yet implemented for ST_LineInterpolatePoint".to_string(),
            )
            .into());
        }
    };

    let result = line_interpolate_array(&geo_array, fraction, coord_type)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn line_interpolate_array(
    array: &dyn GeoArrowArray,
    fraction: f64,
    coord_type: CoordType,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(array, _line_interpolate_impl, fraction, coord_type)
}

fn _line_interpolate_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    fraction: f64,
    coord_type: CoordType,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    use geo::LineInterpolatePoint as _;

    let point_type = PointType::new(Dimension::XY, Default::default()).with_coord_type(coord_type);
    let mut builder = PointBuilder::new(point_type);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            match geo_geom {
                geo::Geometry::LineString(ref ls) => {
                    let pt = ls.line_interpolate_point(fraction);
                    match pt {
                        Some(p) => builder.push_point(Some(&p)),
                        None => builder.push_null(),
                    }
                }
                _ => builder.push_null(),
            }
        } else {
            builder.push_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

// ---------------------------------------------------------------------------
// ST_LineLocatePoint
// ---------------------------------------------------------------------------

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct LineLocatePoint {
    signature: Signature,
}

impl LineLocatePoint {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl Default for LineLocatePoint {
    fn default() -> Self {
        Self::new()
    }
}

static LLP_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for LineLocatePoint {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_linelocatepoint"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(arrow_schema::Field::new("", DataType::Float64, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(line_locate_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(LLP_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a float between 0 and 1 representing the location of the closest point on a LineString to the given Point, as a fraction of the total 2D line length.",
                "ST_LineLocatePoint(linestring, point)",
            )
            .with_argument("geom", "geometry (LineString)")
            .with_argument("point", "geometry (Point)")
            .build()
        }))
    }
}

fn line_locate_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let left_arr = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let right_arr = from_arrow_array(&arrays[1], &args.arg_fields[1])?;

    let result = line_locate_arrays(&left_arr, &right_arr)?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn line_locate_arrays(
    left: &dyn GeoArrowArray,
    right: &dyn GeoArrowArray,
) -> GeoArrowResult<arrow_array::Float64Array> {
    downcast_geoarrow_array!(left, _line_locate_left_impl, right)
}

fn _line_locate_left_impl<'a>(
    left: &'a impl GeoArrowArrayAccessor<'a>,
    right: &dyn GeoArrowArray,
) -> GeoArrowResult<arrow_array::Float64Array> {
    downcast_geoarrow_array!(right, __line_locate_impl, left)
}

fn __line_locate_impl<'a, 'b>(
    right: &'b impl GeoArrowArrayAccessor<'b>,
    left: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<arrow_array::Float64Array> {
    use geo::LineLocatePoint as _;

    let mut builder = Float64Builder::with_capacity(left.len());

    for (l, r) in left.iter().zip(right.iter()) {
        match (l, r) {
            (Some(left_geom), Some(right_geom)) => {
                let left_geo = geometry_to_geo(&left_geom?)?;
                let right_geo = geometry_to_geo(&right_geom?)?;
                match (&left_geo, &right_geo) {
                    (geo::Geometry::LineString(ls), geo::Geometry::Point(pt)) => {
                        let frac = ls.line_locate_point(pt);
                        match frac {
                            Some(f) => builder.append_value(f),
                            None => builder.append_null(),
                        }
                    }
                    _ => builder.append_null(),
                }
            }
            _ => builder.append_null(),
        }
    }

    Ok(builder.finish())
}

// ---------------------------------------------------------------------------
// ST_LineSubstring
// ---------------------------------------------------------------------------

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct LineSubstring {
    signature: Signature,
    coord_type: CoordType,
}

impl LineSubstring {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
            coord_type,
        }
    }
}

impl Default for LineSubstring {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static LS_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for LineSubstring {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_linesubstring"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let metadata =
            Arc::new(Metadata::try_from(args.arg_fields[0].as_ref()).unwrap_or_default());
        let output_type = GeometryType::new(metadata).with_coord_type(self.coord_type);
        Ok(Arc::new(output_type.to_field("", true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(line_substring_impl(args, self.coord_type)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(LS_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the portion of a line between two fractional locations. The first argument must be a LineString. The second and third arguments are floats between 0 and 1 representing the start and end fractions.",
                "ST_LineSubstring(linestring, startfraction, endfraction)",
            )
            .with_argument("geom", "geometry (LineString)")
            .with_argument("startfraction", "float8 - value between 0 and 1")
            .with_argument("endfraction", "float8 - value between 0 and 1")
            .build()
        }))
    }
}

fn line_substring_impl(
    args: ScalarFunctionArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args[0..1])?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;

    let start_fraction = match args.args[1].cast_to(&DataType::Float64, None)? {
        ColumnarValue::Scalar(ScalarValue::Float64(Some(f))) => f,
        _ => {
            return Err(DataFusionError::NotImplemented(
                "Vectorized startfraction not yet implemented for ST_LineSubstring".to_string(),
            )
            .into());
        }
    };

    let end_fraction = match args.args[2].cast_to(&DataType::Float64, None)? {
        ColumnarValue::Scalar(ScalarValue::Float64(Some(f))) => f,
        _ => {
            return Err(DataFusionError::NotImplemented(
                "Vectorized endfraction not yet implemented for ST_LineSubstring".to_string(),
            )
            .into());
        }
    };

    let result = line_substring_array(&geo_array, start_fraction, end_fraction, coord_type)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn line_substring_array(
    array: &dyn GeoArrowArray,
    start_fraction: f64,
    end_fraction: f64,
    coord_type: CoordType,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(
        array,
        _line_substring_impl,
        start_fraction,
        end_fraction,
        coord_type
    )
}

fn _line_substring_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    start_fraction: f64,
    end_fraction: f64,
    coord_type: CoordType,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    use geo::{EuclideanDistance, EuclideanLength, LineInterpolatePoint as _};

    let geom_type = GeometryType::new(Default::default()).with_coord_type(coord_type);
    let mut builder = GeometryBuilder::new(geom_type);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            match geo_geom {
                geo::Geometry::LineString(ref ls) => {
                    let total_length = ls.euclidean_length();
                    if total_length == 0.0 {
                        builder.push_null();
                        continue;
                    }

                    let start_dist = start_fraction * total_length;
                    let end_dist = end_fraction * total_length;

                    let mut coords: Vec<geo::Coord> = Vec::new();

                    // Add the interpolated start point
                    if let Some(start_pt) = ls.line_interpolate_point(start_fraction) {
                        coords.push(start_pt.into());
                    } else {
                        builder.push_null();
                        continue;
                    }

                    // Add interior vertices that fall between start and end fractions
                    let mut cumulative = 0.0;
                    for i in 1..ls.0.len() {
                        let p1 = geo::Point::from(ls.0[i - 1]);
                        let p2 = geo::Point::from(ls.0[i]);
                        let segment_len = p1.euclidean_distance(&p2);
                        cumulative += segment_len;

                        if cumulative > start_dist && cumulative < end_dist {
                            coords.push(ls.0[i]);
                        }
                    }

                    // Add the interpolated end point
                    if let Some(end_pt) = ls.line_interpolate_point(end_fraction) {
                        coords.push(end_pt.into());
                    } else {
                        builder.push_null();
                        continue;
                    }

                    let result_ls = geo::LineString::new(coords);
                    builder.push_geometry(Some(&geo::Geometry::LineString(result_ls)))?;
                }
                _ => builder.push_null(),
            }
        } else {
            builder.push_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod test {
    use arrow_array::cast::AsArray;
    use arrow_array::types::Float64Type;
    use approx::relative_eq;
    use datafusion::prelude::SessionContext;
    use geo_traits::{CoordTrait, PointTrait};
    use geoarrow_array::GeoArrowArrayAccessor;
    use geoarrow_array::array::PointArray;

    use super::*;
    use crate::udf::native::io::{AsText, GeomFromText};

    #[tokio::test]
    async fn test_line_interpolate_point() {
        let ctx = SessionContext::new();

        ctx.register_udf(LineInterpolatePoint::default().into());
        ctx.register_udf(GeomFromText::default().into());

        let df = ctx
            .sql(
                "SELECT ST_LineInterpolatePoint(ST_GeomFromText('LINESTRING(0 0, 10 0)'), 0.5);",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let geo_arr =
            PointArray::try_from((batch.column(0).as_ref(), batch.schema().field(0))).unwrap();
        let point = geo_arr.value(0).unwrap();
        assert!(relative_eq!(point.coord().unwrap().x(), 5.0));
        assert!(relative_eq!(point.coord().unwrap().y(), 0.0));
    }

    #[tokio::test]
    async fn test_line_locate_point() {
        let ctx = SessionContext::new();

        ctx.register_udf(LineLocatePoint::default().into());
        ctx.register_udf(GeomFromText::default().into());

        let df = ctx
            .sql(
                "SELECT ST_LineLocatePoint(ST_GeomFromText('LINESTRING(0 0, 10 0)'), ST_GeomFromText('POINT(5 0)'));",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        let frac = col.as_primitive::<Float64Type>().value(0);
        assert!(relative_eq!(frac, 0.5));
    }

    #[tokio::test]
    async fn test_line_substring() {
        let ctx = SessionContext::new();

        ctx.register_udf(LineSubstring::default().into());
        ctx.register_udf(GeomFromText::default().into());
        ctx.register_udf(AsText.into());

        let df = ctx
            .sql(
                "SELECT ST_AsText(ST_LineSubstring(ST_GeomFromText('LINESTRING(0 0, 10 0)'), 0.25, 0.75));",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        let wkt = col.as_string::<i32>().value(0);
        assert_eq!(wkt, "LINESTRING(2.5 0,7.5 0)");
    }
}

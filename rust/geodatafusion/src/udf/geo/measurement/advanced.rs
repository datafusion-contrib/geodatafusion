use std::any::Any;
use std::sync::{Arc, LazyLock, OnceLock};

use arrow_array::Float64Array;
use arrow_array::builder::Float64Builder;
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_expr_geo::util::to_geo::geometry_to_geo;
use geoarrow_schema::error::GeoArrowResult;
use geoarrow_schema::{CoordType, Dimension, Metadata, PointType};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;

// ---------------------------------------------------------------------------
// ST_Perimeter
// ---------------------------------------------------------------------------

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Perimeter;

impl Perimeter {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for Perimeter {
    fn default() -> Self {
        Self::new()
    }
}

static PERIMETER_ALIASES: LazyLock<Vec<String>> =
    LazyLock::new(|| vec!["st_perimeter2d".to_string()]);
static PERIMETER_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Perimeter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_perimeter"
    }

    fn aliases(&self) -> &[String] {
        PERIMETER_ALIASES.as_slice()
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(perimeter_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(PERIMETER_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the 2D perimeter of a polygon or multipolygon geometry. For non-areal geometries returns 0.",
                "ST_Perimeter(geom)",
            )
            .with_argument("geom", "geometry")
            .build()
        }))
    }
}

fn perimeter_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let result = perimeter_array(&geo_array)?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn perimeter_array(array: &dyn GeoArrowArray) -> GeoArrowResult<Float64Array> {
    downcast_geoarrow_array!(array, _perimeter_typed)
}

fn _perimeter_typed<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<Float64Array> {
    use geo::EuclideanLength;

    let mut builder = Float64Builder::with_capacity(array.len());
    for geom_opt in array.iter() {
        match geom_opt {
            Some(geom) => {
                let geo_geom = geometry_to_geo(&geom?)?;
                let perimeter = match &geo_geom {
                    geo::Geometry::Polygon(p) => {
                        let exterior = p.exterior().euclidean_length();
                        let interiors: f64 =
                            p.interiors().iter().map(|r| r.euclidean_length()).sum();
                        exterior + interiors
                    }
                    geo::Geometry::MultiPolygon(mp) => {
                        mp.0.iter()
                            .map(|p| {
                                let exterior = p.exterior().euclidean_length();
                                let interiors: f64 =
                                    p.interiors().iter().map(|r| r.euclidean_length()).sum();
                                exterior + interiors
                            })
                            .sum()
                    }
                    _ => 0.0,
                };
                builder.append_value(perimeter);
            }
            None => builder.append_null(),
        }
    }
    Ok(builder.finish())
}

// ---------------------------------------------------------------------------
// ST_Azimuth
// ---------------------------------------------------------------------------

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Azimuth;

impl Azimuth {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for Azimuth {
    fn default() -> Self {
        Self::new()
    }
}

static AZIMUTH_SIGNATURE: LazyLock<Signature> =
    LazyLock::new(|| Signature::any(2, Volatility::Immutable));
static AZIMUTH_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Azimuth {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_azimuth"
    }

    fn signature(&self) -> &Signature {
        &AZIMUTH_SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(azimuth_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(AZIMUTH_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the azimuth in radians of the segment defined by the two given point geometries. The azimuth is measured clockwise from north (0 radians).",
                "ST_Azimuth(pointA, pointB)",
            )
            .with_argument("pointA", "geometry (point)")
            .with_argument("pointB", "geometry (point)")
            .build()
        }))
    }
}

fn azimuth_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let left_arr = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let right_arr = from_arrow_array(&arrays[1], &args.arg_fields[1])?;
    let result = azimuth_arrays(&left_arr, &right_arr)?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn azimuth_arrays(
    left: &dyn GeoArrowArray,
    right: &dyn GeoArrowArray,
) -> GeoArrowResult<Float64Array> {
    downcast_geoarrow_array!(left, _azimuth_left, right)
}

fn _azimuth_left<'a>(
    left: &'a impl GeoArrowArrayAccessor<'a>,
    right: &dyn GeoArrowArray,
) -> GeoArrowResult<Float64Array> {
    downcast_geoarrow_array!(right, _azimuth_both, left)
}

fn _azimuth_both<'a, 'b>(
    right: &'b impl GeoArrowArrayAccessor<'b>,
    left: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<Float64Array> {
    let mut builder = Float64Builder::with_capacity(left.len());
    for (l, r) in left.iter().zip(right.iter()) {
        match (l, r) {
            (Some(lg), Some(rg)) => {
                let left_geo = geometry_to_geo(&lg?)?;
                let right_geo = geometry_to_geo(&rg?)?;
                match (&left_geo, &right_geo) {
                    (geo::Geometry::Point(pa), geo::Geometry::Point(pb)) => {
                        let dx = pb.x() - pa.x();
                        let dy = pb.y() - pa.y();
                        // atan2(dx, dy) gives angle from north (positive Y),
                        // clockwise positive
                        let mut azimuth = dx.atan2(dy);
                        if azimuth < 0.0 {
                            azimuth += 2.0 * std::f64::consts::PI;
                        }
                        builder.append_value(azimuth);
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
// ST_HausdorffDistance
// ---------------------------------------------------------------------------

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct HausdorffDistance;

impl HausdorffDistance {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for HausdorffDistance {
    fn default() -> Self {
        Self::new()
    }
}

static HAUSDORFF_SIGNATURE: LazyLock<Signature> =
    LazyLock::new(|| Signature::any(2, Volatility::Immutable));
static HAUSDORFF_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for HausdorffDistance {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_hausdorffdistance"
    }

    fn signature(&self) -> &Signature {
        &HAUSDORFF_SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(hausdorff_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(HAUSDORFF_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the Hausdorff distance between two geometries. The Hausdorff distance is the greatest distance between any point in one geometry and the closest point in the other geometry.",
                "ST_HausdorffDistance(geomA, geomB)",
            )
            .with_argument("geomA", "geometry")
            .with_argument("geomB", "geometry")
            .build()
        }))
    }
}

fn hausdorff_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let left_arr = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let right_arr = from_arrow_array(&arrays[1], &args.arg_fields[1])?;
    let result = hausdorff_arrays(&left_arr, &right_arr)?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn hausdorff_arrays(
    left: &dyn GeoArrowArray,
    right: &dyn GeoArrowArray,
) -> GeoArrowResult<Float64Array> {
    downcast_geoarrow_array!(left, _hausdorff_left, right)
}

fn _hausdorff_left<'a>(
    left: &'a impl GeoArrowArrayAccessor<'a>,
    right: &dyn GeoArrowArray,
) -> GeoArrowResult<Float64Array> {
    downcast_geoarrow_array!(right, _hausdorff_both, left)
}

fn _hausdorff_both<'a, 'b>(
    right: &'b impl GeoArrowArrayAccessor<'b>,
    left: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<Float64Array> {
    use geo::HausdorffDistance as GeoHausdorff;

    let mut builder = Float64Builder::with_capacity(left.len());
    for (l, r) in left.iter().zip(right.iter()) {
        match (l, r) {
            (Some(lg), Some(rg)) => {
                let left_geo = geometry_to_geo(&lg?)?;
                let right_geo = geometry_to_geo(&rg?)?;
                let dist = left_geo.hausdorff_distance(&right_geo);
                builder.append_value(dist);
            }
            _ => builder.append_null(),
        }
    }
    Ok(builder.finish())
}

// ---------------------------------------------------------------------------
// ST_FrechetDistance
// ---------------------------------------------------------------------------

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct FrechetDistance;

impl FrechetDistance {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for FrechetDistance {
    fn default() -> Self {
        Self::new()
    }
}

static FRECHET_SIGNATURE: LazyLock<Signature> =
    LazyLock::new(|| Signature::any(2, Volatility::Immutable));
static FRECHET_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for FrechetDistance {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_frechetdistance"
    }

    fn signature(&self) -> &Signature {
        &FRECHET_SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(frechet_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(FRECHET_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the discrete Frechet distance between two geometries. The Frechet distance is a measure of similarity between curves that accounts for the ordering and location of the points along the curves.",
                "ST_FrechetDistance(geomA, geomB)",
            )
            .with_argument("geomA", "geometry")
            .with_argument("geomB", "geometry")
            .build()
        }))
    }
}

fn frechet_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let left_arr = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let right_arr = from_arrow_array(&arrays[1], &args.arg_fields[1])?;
    let result = frechet_arrays(&left_arr, &right_arr)?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn frechet_arrays(
    left: &dyn GeoArrowArray,
    right: &dyn GeoArrowArray,
) -> GeoArrowResult<Float64Array> {
    downcast_geoarrow_array!(left, _frechet_left, right)
}

fn _frechet_left<'a>(
    left: &'a impl GeoArrowArrayAccessor<'a>,
    right: &dyn GeoArrowArray,
) -> GeoArrowResult<Float64Array> {
    downcast_geoarrow_array!(right, _frechet_both, left)
}

fn _frechet_both<'a, 'b>(
    right: &'b impl GeoArrowArrayAccessor<'b>,
    left: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<Float64Array> {
    use geo::FrechetDistance as GeoFrechet;

    let mut builder = Float64Builder::with_capacity(left.len());
    for (l, r) in left.iter().zip(right.iter()) {
        match (l, r) {
            (Some(lg), Some(rg)) => {
                let left_geo = geometry_to_geo(&lg?)?;
                let right_geo = geometry_to_geo(&rg?)?;
                match (&left_geo, &right_geo) {
                    (geo::Geometry::LineString(ls_a), geo::Geometry::LineString(ls_b)) => {
                        let dist = ls_a.frechet_distance(ls_b);
                        builder.append_value(dist);
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
// ST_ClosestPoint
// ---------------------------------------------------------------------------

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct ClosestPoint {
    coord_type: CoordType,
}

impl ClosestPoint {
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for ClosestPoint {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static CLOSEST_POINT_SIGNATURE: LazyLock<Signature> =
    LazyLock::new(|| Signature::any(2, Volatility::Immutable));
static CLOSEST_POINT_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for ClosestPoint {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_closestpoint"
    }

    fn signature(&self) -> &Signature {
        &CLOSEST_POINT_SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(closest_point_return_field(args, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(closest_point_impl(args, self.coord_type)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(CLOSEST_POINT_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the 2D point on geomA that is closest to geomB. This is the point from which the shortest distance between the two geometries is measured.",
                "ST_ClosestPoint(geomA, geomB)",
            )
            .with_argument("geomA", "geometry")
            .with_argument("geomB", "geometry")
            .build()
        }))
    }
}

fn closest_point_return_field(
    args: ReturnFieldArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<FieldRef> {
    let metadata = Arc::new(Metadata::try_from(args.arg_fields[0].as_ref()).unwrap_or_default());
    let output_type = PointType::new(Dimension::XY, metadata).with_coord_type(coord_type);
    Ok(Arc::new(output_type.to_field("", true)))
}

fn closest_point_impl(
    args: ScalarFunctionArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let left_arr = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let right_arr = from_arrow_array(&arrays[1], &args.arg_fields[1])?;
    let result = closest_point_arrays(&left_arr, &right_arr, coord_type)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn closest_point_arrays(
    left: &dyn GeoArrowArray,
    right: &dyn GeoArrowArray,
    coord_type: CoordType,
) -> GeoArrowResult<geoarrow_array::array::PointArray> {
    downcast_geoarrow_array!(left, _closest_point_left, right, coord_type)
}

fn _closest_point_left<'a>(
    left: &'a impl GeoArrowArrayAccessor<'a>,
    right: &dyn GeoArrowArray,
    coord_type: CoordType,
) -> GeoArrowResult<geoarrow_array::array::PointArray> {
    downcast_geoarrow_array!(right, _closest_point_both, left, coord_type)
}

fn _closest_point_both<'a, 'b>(
    right: &'b impl GeoArrowArrayAccessor<'b>,
    left: &'a impl GeoArrowArrayAccessor<'a>,
    coord_type: CoordType,
) -> GeoArrowResult<geoarrow_array::array::PointArray> {
    
    

    let point_type = PointType::new(Dimension::XY, Default::default()).with_coord_type(coord_type);
    let mut builder = geoarrow_array::builder::PointBuilder::new(point_type);

    for (l, r) in left.iter().zip(right.iter()) {
        match (l, r) {
            (Some(lg), Some(rg)) => {
                let left_geo = geometry_to_geo(&lg?)?;
                let right_geo = geometry_to_geo(&rg?)?;
                // Find the closest point on left_geo to right_geo
                // We need a point from right_geo to query against left_geo
                // ClosestPoint trait: left_geo.closest_point(&point) where point is a Point
                // For a general geometry, we use the centroid of right_geo as the query point,
                // but the PostGIS semantics are: point on A closest to B.
                // geo's ClosestPoint takes a Point argument. We need to iterate
                // or use a different approach. We'll compute closest point on A to each
                // vertex of B and pick the minimum distance one.
                let closest = find_closest_point_on_geom(&left_geo, &right_geo);
                match closest {
                    Some(pt) => builder.push_point(Some(&pt)),
                    None => builder.push_null(),
                }
            }
            _ => builder.push_null(),
        }
    }
    Ok(builder.finish())
}

/// Find the point on `geom_a` that is closest to `geom_b`.
///
/// Iterates over all vertices of `geom_b`, queries `geom_a.closest_point(&vertex)`
/// for each, and returns the candidate with the minimum distance to the query vertex.
/// This is vertex-exact: the result is guaranteed to be the closest point on A to
/// one of B's vertices (not necessarily to an edge midpoint of B).
fn find_closest_point_on_geom(
    geom_a: &geo::Geometry,
    geom_b: &geo::Geometry,
) -> Option<geo::Point> {
    use geo::{Closest, ClosestPoint as GeoClosestPoint, CoordsIter, EuclideanDistance};

    let mut best: Option<geo::Point> = None;
    let mut best_dist = f64::INFINITY;

    for coord in geom_b.coords_iter() {
        let query = geo::Point::new(coord.x, coord.y);
        let candidate = match geom_a.closest_point(&query) {
            Closest::SinglePoint(p) | Closest::Intersection(p) => p,
            Closest::Indeterminate => continue,
        };
        let dist = candidate.euclidean_distance(&query);
        if dist < best_dist {
            best_dist = dist;
            best = Some(candidate);
        }
    }
    best
}

// ---------------------------------------------------------------------------
// ST_MaxDistance
// ---------------------------------------------------------------------------

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct MaxDistance;

impl MaxDistance {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for MaxDistance {
    fn default() -> Self {
        Self::new()
    }
}

static MAX_DISTANCE_SIGNATURE: LazyLock<Signature> =
    LazyLock::new(|| Signature::any(2, Volatility::Immutable));
static MAX_DISTANCE_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for MaxDistance {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_maxdistance"
    }

    fn signature(&self) -> &Signature {
        &MAX_DISTANCE_SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(max_distance_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(MAX_DISTANCE_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the maximum 2D Cartesian distance between two geometries, computed over all pairs of vertices.",
                "ST_MaxDistance(geomA, geomB)",
            )
            .with_argument("geomA", "geometry")
            .with_argument("geomB", "geometry")
            .build()
        }))
    }
}

fn max_distance_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let left_arr = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let right_arr = from_arrow_array(&arrays[1], &args.arg_fields[1])?;
    let result = max_distance_arrays(&left_arr, &right_arr)?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn max_distance_arrays(
    left: &dyn GeoArrowArray,
    right: &dyn GeoArrowArray,
) -> GeoArrowResult<Float64Array> {
    downcast_geoarrow_array!(left, _max_distance_left, right)
}

fn _max_distance_left<'a>(
    left: &'a impl GeoArrowArrayAccessor<'a>,
    right: &dyn GeoArrowArray,
) -> GeoArrowResult<Float64Array> {
    downcast_geoarrow_array!(right, _max_distance_both, left)
}

fn _max_distance_both<'a, 'b>(
    right: &'b impl GeoArrowArrayAccessor<'b>,
    left: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<Float64Array> {
    use geo::CoordsIter;
    use geo::EuclideanDistance;

    let mut builder = Float64Builder::with_capacity(left.len());
    for (l, r) in left.iter().zip(right.iter()) {
        match (l, r) {
            (Some(lg), Some(rg)) => {
                let left_geo = geometry_to_geo(&lg?)?;
                let right_geo = geometry_to_geo(&rg?)?;
                let left_coords: Vec<_> = left_geo.coords_iter().collect();
                let right_coords: Vec<_> = right_geo.coords_iter().collect();
                let mut max_dist: f64 = 0.0;
                for lc in &left_coords {
                    let lp = geo::Point::new(lc.x, lc.y);
                    for rc in &right_coords {
                        let rp = geo::Point::new(rc.x, rc.y);
                        let d = lp.euclidean_distance(&rp);
                        if d > max_dist {
                            max_dist = d;
                        }
                    }
                }
                builder.append_value(max_dist);
            }
            _ => builder.append_null(),
        }
    }
    Ok(builder.finish())
}

#[cfg(test)]
mod test {
    use approx::assert_relative_eq;
    use arrow_array::cast::AsArray;
    use arrow_array::types::Float64Type;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::udf::native::io::GeomFromText;

    // -----------------------------------------------------------------------
    // ST_Perimeter tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_perimeter_square() {
        let ctx = SessionContext::new();
        ctx.register_udf(Perimeter::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        // 1x1 square: perimeter = 4
        let df = ctx
            .sql("SELECT ST_Perimeter(ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch.column(0).as_primitive::<Float64Type>().value(0);
        assert_eq!(val, 4.0);
    }

    #[tokio::test]
    async fn test_perimeter_polygon_with_hole() {
        let ctx = SessionContext::new();
        ctx.register_udf(Perimeter::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        // Outer square 10x10 (perimeter 40), inner square 2x2 (perimeter 8) => total 48
        let df = ctx
            .sql("SELECT ST_Perimeter(ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (4 4, 6 4, 6 6, 4 6, 4 4))'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch.column(0).as_primitive::<Float64Type>().value(0);
        assert_eq!(val, 48.0);
    }

    #[tokio::test]
    async fn test_perimeter_point() {
        let ctx = SessionContext::new();
        ctx.register_udf(Perimeter::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        // Point has perimeter 0
        let df = ctx
            .sql("SELECT ST_Perimeter(ST_GeomFromText('POINT(1 2)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch.column(0).as_primitive::<Float64Type>().value(0);
        assert_eq!(val, 0.0);
    }

    #[tokio::test]
    async fn test_perimeter_linestring() {
        let ctx = SessionContext::new();
        ctx.register_udf(Perimeter::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        // LineString has perimeter 0 (not an areal geometry)
        let df = ctx
            .sql("SELECT ST_Perimeter(ST_GeomFromText('LINESTRING(0 0, 1 1)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch.column(0).as_primitive::<Float64Type>().value(0);
        assert_eq!(val, 0.0);
    }

    // -----------------------------------------------------------------------
    // ST_Azimuth tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_azimuth_north() {
        let ctx = SessionContext::new();
        ctx.register_udf(Azimuth::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        // Point directly north => azimuth = 0
        let df = ctx
            .sql("SELECT ST_Azimuth(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(0 1)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch.column(0).as_primitive::<Float64Type>().value(0);
        assert_relative_eq!(val, 0.0, epsilon = 1e-10);
    }

    #[tokio::test]
    async fn test_azimuth_east() {
        let ctx = SessionContext::new();
        ctx.register_udf(Azimuth::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        // Point directly east => azimuth = pi/2
        let df = ctx
            .sql("SELECT ST_Azimuth(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(1 0)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch.column(0).as_primitive::<Float64Type>().value(0);
        assert_relative_eq!(val, std::f64::consts::FRAC_PI_2, epsilon = 1e-10);
    }

    #[tokio::test]
    async fn test_azimuth_south() {
        let ctx = SessionContext::new();
        ctx.register_udf(Azimuth::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        // Point directly south => azimuth = pi
        let df = ctx
            .sql(
                "SELECT ST_Azimuth(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(0 -1)'));",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch.column(0).as_primitive::<Float64Type>().value(0);
        assert_relative_eq!(val, std::f64::consts::PI, epsilon = 1e-10);
    }

    #[tokio::test]
    async fn test_azimuth_west() {
        let ctx = SessionContext::new();
        ctx.register_udf(Azimuth::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        // Point directly west => azimuth = 3*pi/2
        let df = ctx
            .sql("SELECT ST_Azimuth(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(-1 0)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch.column(0).as_primitive::<Float64Type>().value(0);
        assert_relative_eq!(val, 3.0 * std::f64::consts::FRAC_PI_2, epsilon = 1e-10);
    }

    // -----------------------------------------------------------------------
    // ST_HausdorffDistance tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_hausdorff_distance_identical() {
        let ctx = SessionContext::new();
        ctx.register_udf(HausdorffDistance::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        // Identical geometries => Hausdorff distance = 0
        let df = ctx
            .sql("SELECT ST_HausdorffDistance(ST_GeomFromText('LINESTRING(0 0, 1 1)'), ST_GeomFromText('LINESTRING(0 0, 1 1)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch.column(0).as_primitive::<Float64Type>().value(0);
        assert_eq!(val, 0.0);
    }

    #[tokio::test]
    async fn test_hausdorff_distance_simple() {
        let ctx = SessionContext::new();
        ctx.register_udf(HausdorffDistance::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        // LINESTRING(0 0, 1 0) vs LINESTRING(0 1, 1 1)
        // Every point on A is distance 1 from B, every point on B is distance 1 from A
        // Hausdorff distance = 1
        let df = ctx
            .sql("SELECT ST_HausdorffDistance(ST_GeomFromText('LINESTRING(0 0, 1 0)'), ST_GeomFromText('LINESTRING(0 1, 1 1)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch.column(0).as_primitive::<Float64Type>().value(0);
        assert_relative_eq!(val, 1.0, epsilon = 1e-10);
    }

    #[tokio::test]
    async fn test_hausdorff_distance_points() {
        let ctx = SessionContext::new();
        ctx.register_udf(HausdorffDistance::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        // POINT(0 0) vs POINT(3 4) => distance = 5
        let df = ctx
            .sql("SELECT ST_HausdorffDistance(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(3 4)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch.column(0).as_primitive::<Float64Type>().value(0);
        assert_relative_eq!(val, 5.0, epsilon = 1e-10);
    }

    // -----------------------------------------------------------------------
    // ST_FrechetDistance tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_frechet_distance_identical() {
        let ctx = SessionContext::new();
        ctx.register_udf(FrechetDistance::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        // Identical linestrings => Frechet distance = 0
        let df = ctx
            .sql("SELECT ST_FrechetDistance(ST_GeomFromText('LINESTRING(0 0, 1 1)'), ST_GeomFromText('LINESTRING(0 0, 1 1)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch.column(0).as_primitive::<Float64Type>().value(0);
        assert_eq!(val, 0.0);
    }

    #[tokio::test]
    async fn test_frechet_distance_simple() {
        let ctx = SessionContext::new();
        ctx.register_udf(FrechetDistance::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        // LINESTRING(0 0, 1 0) vs LINESTRING(0 1, 1 1) => Frechet distance = 1
        let df = ctx
            .sql("SELECT ST_FrechetDistance(ST_GeomFromText('LINESTRING(0 0, 1 0)'), ST_GeomFromText('LINESTRING(0 1, 1 1)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch.column(0).as_primitive::<Float64Type>().value(0);
        assert_relative_eq!(val, 1.0, epsilon = 1e-10);
    }

    // -----------------------------------------------------------------------
    // ST_ClosestPoint tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_closest_point_on_line() {
        use geo_traits::{CoordTrait, PointTrait};
        use geoarrow_array::array::PointArray;

        let ctx = SessionContext::new();
        ctx.register_udf(ClosestPoint::default().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        // Closest point on LINESTRING(0 0, 10 0) to POINT(5 5) should be POINT(5 0)
        let df = ctx
            .sql("SELECT ST_ClosestPoint(ST_GeomFromText('LINESTRING(0 0, 10 0)'), ST_GeomFromText('POINT(5 5)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let geo_arr =
            PointArray::try_from((batch.column(0).as_ref(), batch.schema().field(0))).unwrap();
        let point = geo_arr.value(0).unwrap();
        assert_relative_eq!(point.coord().unwrap().x(), 5.0, epsilon = 1e-10);
        assert_relative_eq!(point.coord().unwrap().y(), 0.0, epsilon = 1e-10);
    }

    #[tokio::test]
    async fn test_closest_point_coincident() {
        use geo_traits::{CoordTrait, PointTrait};
        use geoarrow_array::array::PointArray;

        let ctx = SessionContext::new();
        ctx.register_udf(ClosestPoint::default().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        // Point on the line itself
        let df = ctx
            .sql("SELECT ST_ClosestPoint(ST_GeomFromText('LINESTRING(0 0, 10 0)'), ST_GeomFromText('POINT(3 0)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let geo_arr =
            PointArray::try_from((batch.column(0).as_ref(), batch.schema().field(0))).unwrap();
        let point = geo_arr.value(0).unwrap();
        assert_relative_eq!(point.coord().unwrap().x(), 3.0, epsilon = 1e-10);
        assert_relative_eq!(point.coord().unwrap().y(), 0.0, epsilon = 1e-10);
    }

    // -----------------------------------------------------------------------
    // ST_MaxDistance tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_max_distance_points() {
        let ctx = SessionContext::new();
        ctx.register_udf(MaxDistance::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        // POINT(0 0) vs POINT(3 4) => max distance = 5
        let df = ctx
            .sql("SELECT ST_MaxDistance(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(3 4)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch.column(0).as_primitive::<Float64Type>().value(0);
        assert_relative_eq!(val, 5.0, epsilon = 1e-10);
    }

    #[tokio::test]
    async fn test_max_distance_line_to_point() {
        let ctx = SessionContext::new();
        ctx.register_udf(MaxDistance::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        // LINESTRING(0 0, 3 0) vs POINT(0 4)
        // Max distance is from (3,0) to (0,4) = 5
        let df = ctx
            .sql("SELECT ST_MaxDistance(ST_GeomFromText('LINESTRING(0 0, 3 0)'), ST_GeomFromText('POINT(0 4)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch.column(0).as_primitive::<Float64Type>().value(0);
        assert_relative_eq!(val, 5.0, epsilon = 1e-10);
    }

    #[tokio::test]
    async fn test_max_distance_same_geometry() {
        let ctx = SessionContext::new();
        ctx.register_udf(MaxDistance::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        // Square: max distance is the diagonal = sqrt(2)
        let df = ctx
            .sql("SELECT ST_MaxDistance(ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'), ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch.column(0).as_primitive::<Float64Type>().value(0);
        assert_relative_eq!(val, std::f64::consts::SQRT_2, epsilon = 1e-10);
    }
}

//! Geometry editor functions: ST_Reverse, ST_FlipCoordinates, ST_Force2D,
//! ST_Multi, ST_Normalize, ST_RemoveRepeatedPoints, ST_CollectionExtract.

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
// ST_Reverse
// =========================================================================
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Reverse {
    coord_type: CoordType,
}

impl Reverse {
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for Reverse {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static REVERSE_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Reverse {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_reverse"
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
        Ok(reverse_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(REVERSE_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the geometry with vertex order reversed.",
                "ST_Reverse(geometry)",
            )
            .with_argument("geom", "geometry")
            .build()
        }))
    }
}

fn reverse_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let result = reverse_array(&geo_array)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn reverse_array(array: &dyn GeoArrowArray) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(array, _reverse_impl)
}

fn reverse_line_string(ls: &geo::LineString) -> geo::LineString {
    geo::LineString(ls.0.iter().rev().cloned().collect())
}

fn reverse_polygon(poly: &geo::Polygon) -> geo::Polygon {
    let exterior = reverse_line_string(poly.exterior());
    let interiors = poly.interiors().iter().map(reverse_line_string).collect();
    geo::Polygon::new(exterior, interiors)
}

fn reverse_geometry(geom: &geo::Geometry) -> geo::Geometry {
    match geom {
        geo::Geometry::Point(p) => geo::Geometry::Point(*p),
        geo::Geometry::Line(l) => geo::Geometry::Line(geo::Line::new(l.end, l.start)),
        geo::Geometry::LineString(ls) => geo::Geometry::LineString(reverse_line_string(ls)),
        geo::Geometry::Polygon(p) => geo::Geometry::Polygon(reverse_polygon(p)),
        geo::Geometry::MultiPoint(mp) => {
            geo::Geometry::MultiPoint(geo::MultiPoint(mp.0.iter().rev().cloned().collect()))
        }
        geo::Geometry::MultiLineString(mls) => geo::Geometry::MultiLineString(
            geo::MultiLineString(mls.0.iter().map(reverse_line_string).collect()),
        ),
        geo::Geometry::MultiPolygon(mp) => geo::Geometry::MultiPolygon(geo::MultiPolygon(
            mp.0.iter().map(reverse_polygon).collect(),
        )),
        geo::Geometry::GeometryCollection(gc) => geo::Geometry::GeometryCollection(
            geo::GeometryCollection(gc.0.iter().map(reverse_geometry).collect()),
        ),
        geo::Geometry::Rect(r) => geo::Geometry::Rect(*r),
        geo::Geometry::Triangle(t) => {
            geo::Geometry::Triangle(geo::Triangle(t.2, t.1, t.0))
        }
    }
}

fn _reverse_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let geom_type = GeometryType::new(Default::default());
    let mut builder = GeometryBuilder::new(geom_type);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let reversed = reverse_geometry(&geo_geom);
            builder.push_geometry(Some(&reversed))?;
        } else {
            builder.push_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

// =========================================================================
// ST_FlipCoordinates
// =========================================================================
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct FlipCoordinates {
    coord_type: CoordType,
}

impl FlipCoordinates {
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for FlipCoordinates {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static FLIP_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for FlipCoordinates {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_flipcoordinates"
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
        Ok(flip_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(FLIP_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a version of the given geometry with X and Y axis flipped.",
                "ST_FlipCoordinates(geometry)",
            )
            .with_argument("geom", "geometry")
            .build()
        }))
    }
}

fn flip_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let result = flip_array(&geo_array)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn flip_array(array: &dyn GeoArrowArray) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(array, _flip_impl)
}

fn _flip_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    use geo::MapCoords;

    let geom_type = GeometryType::new(Default::default());
    let mut builder = GeometryBuilder::new(geom_type);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let flipped = geo_geom.map_coords(|coord| geo::Coord {
                x: coord.y,
                y: coord.x,
            });
            builder.push_geometry(Some(&flipped))?;
        } else {
            builder.push_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

// =========================================================================
// ST_Force2D
// =========================================================================
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Force2D {
    coord_type: CoordType,
}

impl Force2D {
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for Force2D {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static FORCE2D_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Force2D {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_force2d"
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
        Ok(force2d_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(FORCE2D_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Forces the geometries into a 2-dimensional mode so that all output representations will only have X and Y coordinates.",
                "ST_Force2D(geometry)",
            )
            .with_argument("geom", "geometry")
            .build()
        }))
    }
}

fn force2d_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let result = force2d_array(&geo_array)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn force2d_array(array: &dyn GeoArrowArray) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(array, _force2d_impl)
}

fn _force2d_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    use geo::MapCoords;

    let geom_type = GeometryType::new(Default::default());
    let mut builder = GeometryBuilder::new(geom_type);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            // geometry_to_geo already produces 2D geo types (only x,y),
            // so we just pass it through to ensure the output type is XY.
            let forced = geo_geom.map_coords(|coord| geo::Coord {
                x: coord.x,
                y: coord.y,
            });
            builder.push_geometry(Some(&forced))?;
        } else {
            builder.push_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

// =========================================================================
// ST_Multi
// =========================================================================
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Multi {
    coord_type: CoordType,
}

impl Multi {
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for Multi {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static MULTI_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Multi {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_multi"
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
        Ok(multi_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(MULTI_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the geometry as a MULTI* geometry. If the geometry is already a MULTI*, it is returned unchanged.",
                "ST_Multi(geometry)",
            )
            .with_argument("geom", "geometry")
            .build()
        }))
    }
}

fn multi_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let result = multi_array(&geo_array)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn multi_array(array: &dyn GeoArrowArray) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(array, _multi_impl)
}

fn _multi_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let geom_type = GeometryType::new(Default::default());
    let mut builder = GeometryBuilder::new(geom_type);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let multi = match geo_geom {
                geo::Geometry::Point(p) => {
                    geo::Geometry::MultiPoint(geo::MultiPoint(vec![p]))
                }
                geo::Geometry::LineString(ls) => {
                    geo::Geometry::MultiLineString(geo::MultiLineString(vec![ls]))
                }
                geo::Geometry::Polygon(p) => {
                    geo::Geometry::MultiPolygon(geo::MultiPolygon(vec![p]))
                }
                // Already multi types or other types pass through unchanged
                other => other,
            };
            builder.push_geometry(Some(&multi))?;
        } else {
            builder.push_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

// =========================================================================
// ST_Normalize
// =========================================================================
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Normalize {
    coord_type: CoordType,
}

impl Normalize {
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for Normalize {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static NORMALIZE_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Normalize {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_normalize"
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
        Ok(normalize_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(NORMALIZE_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the geometry in its normalized/canonical form. May reorder vertices in polygon rings, elements in multi-geometries, and so on.",
                "ST_Normalize(geometry)",
            )
            .with_argument("geom", "geometry")
            .build()
        }))
    }
}

fn normalize_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let result = normalize_array(&geo_array)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn normalize_array(array: &dyn GeoArrowArray) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(array, _normalize_impl)
}

fn normalize_line_string(ls: &geo::LineString) -> geo::LineString {
    ls.clone()
}

fn normalize_polygon(poly: &geo::Polygon) -> geo::Polygon {
    // Normalize exterior ring: rotate so the lexicographically smallest point is first
    let exterior = normalize_ring(poly.exterior());
    let mut interiors: Vec<geo::LineString> =
        poly.interiors().iter().map(normalize_ring).collect();
    // Sort interior rings lexicographically for canonical ordering
    interiors.sort_by(|a, b| {
        for (ca, cb) in a.0.iter().zip(b.0.iter()) {
            let cmp_x = ca.x.partial_cmp(&cb.x).unwrap_or(std::cmp::Ordering::Equal);
            if cmp_x != std::cmp::Ordering::Equal {
                return cmp_x;
            }
            let cmp_y = ca.y.partial_cmp(&cb.y).unwrap_or(std::cmp::Ordering::Equal);
            if cmp_y != std::cmp::Ordering::Equal {
                return cmp_y;
            }
        }
        a.0.len().cmp(&b.0.len())
    });
    geo::Polygon::new(exterior, interiors)
}

fn normalize_ring(ring: &geo::LineString) -> geo::LineString {
    if ring.0.len() <= 1 {
        return ring.clone();
    }

    // For a closed ring, skip the closing coordinate for rotation
    let coords = if ring.0.first() == ring.0.last() && ring.0.len() > 1 {
        &ring.0[..ring.0.len() - 1]
    } else {
        &ring.0
    };

    if coords.is_empty() {
        return ring.clone();
    }

    // Find the index of the lexicographically smallest coordinate
    let min_idx = coords
        .iter()
        .enumerate()
        .min_by(|(_, a), (_, b)| {
            a.x.partial_cmp(&b.x)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.y.partial_cmp(&b.y).unwrap_or(std::cmp::Ordering::Equal))
        })
        .map(|(i, _)| i)
        .unwrap_or(0);

    // Rotate the ring so the smallest coordinate is first
    let mut rotated: Vec<geo::Coord> = Vec::with_capacity(ring.0.len());
    for i in 0..coords.len() {
        rotated.push(coords[(min_idx + i) % coords.len()]);
    }
    // Re-close the ring
    if ring.0.first() == ring.0.last() && !rotated.is_empty() {
        rotated.push(rotated[0]);
    }

    geo::LineString(rotated)
}

fn normalize_geometry(geom: &geo::Geometry) -> geo::Geometry {
    match geom {
        geo::Geometry::Point(p) => geo::Geometry::Point(*p),
        geo::Geometry::Line(l) => {
            // Normalize line so the smaller point comes first
            if (l.start.x, l.start.y) <= (l.end.x, l.end.y) {
                geo::Geometry::Line(*l)
            } else {
                geo::Geometry::Line(geo::Line::new(l.end, l.start))
            }
        }
        geo::Geometry::LineString(ls) => {
            geo::Geometry::LineString(normalize_line_string(ls))
        }
        geo::Geometry::Polygon(p) => geo::Geometry::Polygon(normalize_polygon(p)),
        geo::Geometry::MultiPoint(mp) => {
            let mut points = mp.0.clone();
            points.sort_by(|a, b| {
                a.x().partial_cmp(&b.x())
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| {
                        a.y().partial_cmp(&b.y())
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
            });
            geo::Geometry::MultiPoint(geo::MultiPoint(points))
        }
        geo::Geometry::MultiLineString(mls) => {
            let lines: Vec<geo::LineString> =
                mls.0.iter().map(normalize_line_string).collect();
            geo::Geometry::MultiLineString(geo::MultiLineString(lines))
        }
        geo::Geometry::MultiPolygon(mp) => {
            let polys: Vec<geo::Polygon> =
                mp.0.iter().map(normalize_polygon).collect();
            geo::Geometry::MultiPolygon(geo::MultiPolygon(polys))
        }
        geo::Geometry::GeometryCollection(gc) => {
            let geoms: Vec<geo::Geometry> =
                gc.0.iter().map(normalize_geometry).collect();
            geo::Geometry::GeometryCollection(geo::GeometryCollection(geoms))
        }
        geo::Geometry::Rect(r) => geo::Geometry::Rect(*r),
        geo::Geometry::Triangle(t) => geo::Geometry::Triangle(*t),
    }
}

fn _normalize_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let geom_type = GeometryType::new(Default::default());
    let mut builder = GeometryBuilder::new(geom_type);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let normalized = normalize_geometry(&geo_geom);
            builder.push_geometry(Some(&normalized))?;
        } else {
            builder.push_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

// =========================================================================
// ST_RemoveRepeatedPoints
// =========================================================================
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct RemoveRepeatedPoints {
    signature: Signature,
    coord_type: CoordType,
}

impl RemoveRepeatedPoints {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    datafusion::logical_expr::TypeSignature::Any(1),
                    datafusion::logical_expr::TypeSignature::Any(2),
                ],
                Volatility::Immutable,
            ),
            coord_type,
        }
    }
}

impl Default for RemoveRepeatedPoints {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static REMOVE_REPEATED_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for RemoveRepeatedPoints {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_removerepeatedpoints"
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
        Ok(remove_repeated_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(REMOVE_REPEATED_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a version of the given geometry with duplicated points removed. An optional tolerance distance parameter can be supplied: points within the tolerance of each other are considered duplicates.",
                "ST_RemoveRepeatedPoints(geometry, tolerance)",
            )
            .with_argument("geom", "geometry")
            .with_argument(
                "tolerance",
                "float8 (optional) - tolerance distance below which points are considered duplicates",
            )
            .build()
        }))
    }
}

fn remove_repeated_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args[0..1])?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;

    let tolerance = if args.args.len() > 1 {
        match args.args[1].cast_to(&DataType::Float64, None)? {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => Some(v),
            _ => None,
        }
    } else {
        None
    };

    let result = remove_repeated_array(&geo_array, tolerance)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn remove_repeated_array(
    array: &dyn GeoArrowArray,
    tolerance: Option<f64>,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(array, _remove_repeated_impl, tolerance)
}

fn remove_repeated_coords(coords: &[geo::Coord], tolerance: Option<f64>) -> Vec<geo::Coord> {
    if coords.is_empty() {
        return vec![];
    }

    let mut result = vec![coords[0]];
    for coord in &coords[1..] {
        let last = result.last().unwrap();
        let dominated = match tolerance {
            Some(tol) => {
                let dx = coord.x - last.x;
                let dy = coord.y - last.y;
                (dx * dx + dy * dy).sqrt() <= tol
            }
            None => coord.x == last.x && coord.y == last.y,
        };
        if !dominated {
            result.push(*coord);
        }
    }
    result
}

fn remove_repeated_line_string(
    ls: &geo::LineString,
    tolerance: Option<f64>,
) -> geo::LineString {
    geo::LineString(remove_repeated_coords(&ls.0, tolerance))
}

fn remove_repeated_polygon(
    poly: &geo::Polygon,
    tolerance: Option<f64>,
) -> geo::Polygon {
    let exterior = remove_repeated_line_string(poly.exterior(), tolerance);
    let interiors = poly
        .interiors()
        .iter()
        .map(|ring| remove_repeated_line_string(ring, tolerance))
        .collect();
    geo::Polygon::new(exterior, interiors)
}

fn remove_repeated_geometry(
    geom: &geo::Geometry,
    tolerance: Option<f64>,
) -> geo::Geometry {
    match geom {
        geo::Geometry::Point(p) => geo::Geometry::Point(*p),
        geo::Geometry::Line(l) => geo::Geometry::Line(*l),
        geo::Geometry::LineString(ls) => {
            geo::Geometry::LineString(remove_repeated_line_string(ls, tolerance))
        }
        geo::Geometry::Polygon(p) => {
            geo::Geometry::Polygon(remove_repeated_polygon(p, tolerance))
        }
        geo::Geometry::MultiPoint(mp) => {
            if mp.0.is_empty() {
                return geo::Geometry::MultiPoint(geo::MultiPoint(vec![]));
            }
            let mut points = vec![mp.0[0]];
            for pt in &mp.0[1..] {
                let last = points.last().unwrap();
                let dominated = match tolerance {
                    Some(tol) => {
                        let dx = pt.x() - last.x();
                        let dy = pt.y() - last.y();
                        (dx * dx + dy * dy).sqrt() <= tol
                    }
                    None => pt.x() == last.x() && pt.y() == last.y(),
                };
                if !dominated {
                    points.push(*pt);
                }
            }
            geo::Geometry::MultiPoint(geo::MultiPoint(points))
        }
        geo::Geometry::MultiLineString(mls) => geo::Geometry::MultiLineString(
            geo::MultiLineString(
                mls.0
                    .iter()
                    .map(|ls| remove_repeated_line_string(ls, tolerance))
                    .collect(),
            ),
        ),
        geo::Geometry::MultiPolygon(mp) => geo::Geometry::MultiPolygon(geo::MultiPolygon(
            mp.0.iter()
                .map(|p| remove_repeated_polygon(p, tolerance))
                .collect(),
        )),
        geo::Geometry::GeometryCollection(gc) => geo::Geometry::GeometryCollection(
            geo::GeometryCollection(
                gc.0.iter()
                    .map(|g| remove_repeated_geometry(g, tolerance))
                    .collect(),
            ),
        ),
        geo::Geometry::Rect(r) => geo::Geometry::Rect(*r),
        geo::Geometry::Triangle(t) => geo::Geometry::Triangle(*t),
    }
}

fn _remove_repeated_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    tolerance: Option<f64>,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let geom_type = GeometryType::new(Default::default());
    let mut builder = GeometryBuilder::new(geom_type);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let cleaned = remove_repeated_geometry(&geo_geom, tolerance);
            builder.push_geometry(Some(&cleaned))?;
        } else {
            builder.push_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

// =========================================================================
// ST_CollectionExtract
// =========================================================================
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct CollectionExtract {
    signature: Signature,
    coord_type: CoordType,
}

impl CollectionExtract {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    datafusion::logical_expr::TypeSignature::Any(1),
                    datafusion::logical_expr::TypeSignature::Any(2),
                ],
                Volatility::Immutable,
            ),
            coord_type,
        }
    }
}

impl Default for CollectionExtract {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static COLLECTION_EXTRACT_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for CollectionExtract {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_collectionextract"
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
        Ok(collection_extract_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(COLLECTION_EXTRACT_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Given a geometry collection, returns a multi-geometry containing only elements of a specified type. Type numbers are: 1 = Point, 2 = LineString, 3 = Polygon. If no type is specified, returns the multi-geometry of the highest-dimension type present.",
                "ST_CollectionExtract(geometry, type)",
            )
            .with_argument("geom", "geometry")
            .with_argument(
                "type",
                "integer (optional) - 1=Point, 2=LineString, 3=Polygon",
            )
            .build()
        }))
    }
}

fn collection_extract_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args[0..1])?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;

    let extract_type = if args.args.len() > 1 {
        match args.args[1].cast_to(&DataType::Int32, None)? {
            ColumnarValue::Scalar(ScalarValue::Int32(Some(v))) => Some(v),
            _ => None,
        }
    } else {
        None
    };

    let result = collection_extract_array(&geo_array, extract_type)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn collection_extract_array(
    array: &dyn GeoArrowArray,
    extract_type: Option<i32>,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(array, _collection_extract_impl, extract_type)
}

fn collect_geometries_flat(geom: &geo::Geometry, out: &mut Vec<geo::Geometry>) {
    match geom {
        geo::Geometry::GeometryCollection(gc) => {
            for g in &gc.0 {
                collect_geometries_flat(g, out);
            }
        }
        other => out.push(other.clone()),
    }
}

fn extract_by_type(geom: &geo::Geometry, extract_type: Option<i32>) -> geo::Geometry {
    // Flatten the geometry collection
    let mut flat = Vec::new();
    collect_geometries_flat(geom, &mut flat);

    let target_type = extract_type.unwrap_or_else(|| {
        // Find highest dimension type present
        let mut max_dim = 0i32;
        for g in &flat {
            let dim = match g {
                geo::Geometry::Point(_) | geo::Geometry::MultiPoint(_) => 1,
                geo::Geometry::Line(_)
                | geo::Geometry::LineString(_)
                | geo::Geometry::MultiLineString(_) => 2,
                geo::Geometry::Polygon(_)
                | geo::Geometry::MultiPolygon(_)
                | geo::Geometry::Rect(_)
                | geo::Geometry::Triangle(_) => 3,
                _ => 0,
            };
            if dim > max_dim {
                max_dim = dim;
            }
        }
        max_dim
    });

    match target_type {
        1 => {
            // Extract points
            let mut points = Vec::new();
            for g in &flat {
                match g {
                    geo::Geometry::Point(p) => points.push(*p),
                    geo::Geometry::MultiPoint(mp) => points.extend(mp.0.iter()),
                    _ => {}
                }
            }
            geo::Geometry::MultiPoint(geo::MultiPoint(points))
        }
        2 => {
            // Extract line strings
            let mut lines = Vec::new();
            for g in &flat {
                match g {
                    geo::Geometry::Line(l) => {
                        lines.push(geo::LineString(vec![l.start, l.end]));
                    }
                    geo::Geometry::LineString(ls) => lines.push(ls.clone()),
                    geo::Geometry::MultiLineString(mls) => lines.extend(mls.0.iter().cloned()),
                    _ => {}
                }
            }
            geo::Geometry::MultiLineString(geo::MultiLineString(lines))
        }
        3 => {
            // Extract polygons
            let mut polygons = Vec::new();
            for g in &flat {
                match g {
                    geo::Geometry::Polygon(p) => polygons.push(p.clone()),
                    geo::Geometry::MultiPolygon(mp) => polygons.extend(mp.0.iter().cloned()),
                    geo::Geometry::Rect(r) => polygons.push(r.to_polygon()),
                    geo::Geometry::Triangle(t) => polygons.push(t.to_polygon()),
                    _ => {}
                }
            }
            geo::Geometry::MultiPolygon(geo::MultiPolygon(polygons))
        }
        _ => {
            // Return empty geometry collection for unknown type
            geo::Geometry::GeometryCollection(geo::GeometryCollection(vec![]))
        }
    }
}

fn _collection_extract_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    extract_type: Option<i32>,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let geom_type = GeometryType::new(Default::default());
    let mut builder = GeometryBuilder::new(geom_type);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let extracted = extract_by_type(&geo_geom, extract_type);
            builder.push_geometry(Some(&extracted))?;
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

#[cfg(test)]
mod test {
    use arrow_array::cast::AsArray;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::udf::native::io::{AsText, GeomFromText};

    #[tokio::test]
    async fn test_reverse_linestring() {
        let ctx = SessionContext::new();
        ctx.register_udf(Reverse::default().into());
        ctx.register_udf(GeomFromText::default().into());
        ctx.register_udf(AsText.into());

        let df = ctx
            .sql(
                "SELECT ST_AsText(ST_Reverse(ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)')));",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch
            .column(0)
            .as_string::<i32>()
            .value(0);
        assert_eq!(val, "LINESTRING(2 2,1 1,0 0)");
    }

    #[tokio::test]
    async fn test_reverse_point() {
        let ctx = SessionContext::new();
        ctx.register_udf(Reverse::default().into());
        ctx.register_udf(GeomFromText::default().into());
        ctx.register_udf(AsText.into());

        let df = ctx
            .sql("SELECT ST_AsText(ST_Reverse(ST_GeomFromText('POINT(1 2)')));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch
            .column(0)
            .as_string::<i32>()
            .value(0);
        assert_eq!(val, "POINT(1 2)");
    }

    #[tokio::test]
    async fn test_flip_coordinates() {
        let ctx = SessionContext::new();
        ctx.register_udf(FlipCoordinates::default().into());
        ctx.register_udf(GeomFromText::default().into());
        ctx.register_udf(AsText.into());

        let df = ctx
            .sql(
                "SELECT ST_AsText(ST_FlipCoordinates(ST_GeomFromText('POINT(1 2)')));",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch
            .column(0)
            .as_string::<i32>()
            .value(0);
        assert_eq!(val, "POINT(2 1)");
    }

    #[tokio::test]
    async fn test_flip_coordinates_linestring() {
        let ctx = SessionContext::new();
        ctx.register_udf(FlipCoordinates::default().into());
        ctx.register_udf(GeomFromText::default().into());
        ctx.register_udf(AsText.into());

        let df = ctx
            .sql(
                "SELECT ST_AsText(ST_FlipCoordinates(ST_GeomFromText('LINESTRING(0 1, 2 3, 4 5)')));",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch
            .column(0)
            .as_string::<i32>()
            .value(0);
        assert_eq!(val, "LINESTRING(1 0,3 2,5 4)");
    }

    #[tokio::test]
    async fn test_force2d() {
        let ctx = SessionContext::new();
        ctx.register_udf(Force2D::default().into());
        ctx.register_udf(GeomFromText::default().into());
        ctx.register_udf(AsText.into());

        let df = ctx
            .sql(
                "SELECT ST_AsText(ST_Force2D(ST_GeomFromText('POINT(1 2)')));",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch
            .column(0)
            .as_string::<i32>()
            .value(0);
        assert_eq!(val, "POINT(1 2)");
    }

    #[tokio::test]
    async fn test_multi_point() {
        let ctx = SessionContext::new();
        ctx.register_udf(Multi::default().into());
        ctx.register_udf(GeomFromText::default().into());
        ctx.register_udf(AsText.into());

        let df = ctx
            .sql("SELECT ST_AsText(ST_Multi(ST_GeomFromText('POINT(1 2)')));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch
            .column(0)
            .as_string::<i32>()
            .value(0);
        assert_eq!(val, "MULTIPOINT((1 2))");
    }

    #[tokio::test]
    async fn test_multi_linestring() {
        let ctx = SessionContext::new();
        ctx.register_udf(Multi::default().into());
        ctx.register_udf(GeomFromText::default().into());
        ctx.register_udf(AsText.into());

        let df = ctx
            .sql(
                "SELECT ST_AsText(ST_Multi(ST_GeomFromText('LINESTRING(0 0, 1 1)')));",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch
            .column(0)
            .as_string::<i32>()
            .value(0);
        assert_eq!(val, "MULTILINESTRING((0 0,1 1))");
    }

    #[tokio::test]
    async fn test_multi_polygon() {
        let ctx = SessionContext::new();
        ctx.register_udf(Multi::default().into());
        ctx.register_udf(GeomFromText::default().into());
        ctx.register_udf(AsText.into());

        let df = ctx
            .sql(
                "SELECT ST_AsText(ST_Multi(ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')));",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch
            .column(0)
            .as_string::<i32>()
            .value(0);
        assert_eq!(val, "MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)))");
    }

    #[tokio::test]
    async fn test_multi_already_multi() {
        let ctx = SessionContext::new();
        ctx.register_udf(Multi::default().into());
        ctx.register_udf(GeomFromText::default().into());
        ctx.register_udf(AsText.into());

        let df = ctx
            .sql(
                "SELECT ST_AsText(ST_Multi(ST_GeomFromText('MULTIPOINT(1 2, 3 4)')));",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch
            .column(0)
            .as_string::<i32>()
            .value(0);
        assert_eq!(val, "MULTIPOINT((1 2),(3 4))");
    }

    #[tokio::test]
    async fn test_normalize() {
        let ctx = SessionContext::new();
        ctx.register_udf(Normalize::default().into());
        ctx.register_udf(GeomFromText::default().into());
        ctx.register_udf(AsText.into());

        // Normalizing a point should return the same point
        let df = ctx
            .sql(
                "SELECT ST_AsText(ST_Normalize(ST_GeomFromText('POINT(1 2)')));",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch
            .column(0)
            .as_string::<i32>()
            .value(0);
        assert_eq!(val, "POINT(1 2)");
    }

    #[tokio::test]
    async fn test_normalize_multipoint() {
        let ctx = SessionContext::new();
        ctx.register_udf(Normalize::default().into());
        ctx.register_udf(GeomFromText::default().into());
        ctx.register_udf(AsText.into());

        // Normalizing a multipoint should sort points lexicographically
        let df = ctx
            .sql(
                "SELECT ST_AsText(ST_Normalize(ST_GeomFromText('MULTIPOINT(3 4, 1 2)')));",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch
            .column(0)
            .as_string::<i32>()
            .value(0);
        assert_eq!(val, "MULTIPOINT((1 2),(3 4))");
    }

    #[tokio::test]
    async fn test_remove_repeated_points() {
        let ctx = SessionContext::new();
        ctx.register_udf(RemoveRepeatedPoints::default().into());
        ctx.register_udf(GeomFromText::default().into());
        ctx.register_udf(AsText.into());

        let df = ctx
            .sql(
                "SELECT ST_AsText(ST_RemoveRepeatedPoints(ST_GeomFromText('LINESTRING(0 0, 0 0, 1 1, 1 1, 2 2)')));",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch
            .column(0)
            .as_string::<i32>()
            .value(0);
        assert_eq!(val, "LINESTRING(0 0,1 1,2 2)");
    }

    #[tokio::test]
    async fn test_remove_repeated_points_with_tolerance() {
        let ctx = SessionContext::new();
        ctx.register_udf(RemoveRepeatedPoints::default().into());
        ctx.register_udf(GeomFromText::default().into());
        ctx.register_udf(AsText.into());

        let df = ctx
            .sql(
                "SELECT ST_AsText(ST_RemoveRepeatedPoints(ST_GeomFromText('LINESTRING(0 0, 0.1 0.1, 1 1, 2 2)'), 0.2));",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch
            .column(0)
            .as_string::<i32>()
            .value(0);
        // 0.1,0.1 is within tolerance 0.2 of 0,0 so it gets removed
        assert_eq!(val, "LINESTRING(0 0,1 1,2 2)");
    }

    #[tokio::test]
    async fn test_collection_extract_polygons() {
        let ctx = SessionContext::new();
        ctx.register_udf(CollectionExtract::default().into());
        ctx.register_udf(GeomFromText::default().into());
        ctx.register_udf(AsText.into());

        let df = ctx
            .sql(
                "SELECT ST_AsText(ST_CollectionExtract(ST_GeomFromText('GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(1 1, 2 2), POLYGON((0 0, 1 0, 1 1, 0 1, 0 0)))'), 3));",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch
            .column(0)
            .as_string::<i32>()
            .value(0);
        assert_eq!(val, "MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)))");
    }

    #[tokio::test]
    async fn test_collection_extract_points() {
        let ctx = SessionContext::new();
        ctx.register_udf(CollectionExtract::default().into());
        ctx.register_udf(GeomFromText::default().into());
        ctx.register_udf(AsText.into());

        let df = ctx
            .sql(
                "SELECT ST_AsText(ST_CollectionExtract(ST_GeomFromText('GEOMETRYCOLLECTION(POINT(0 0), POINT(1 1), LINESTRING(2 2, 3 3))'), 1));",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch
            .column(0)
            .as_string::<i32>()
            .value(0);
        assert_eq!(val, "MULTIPOINT((0 0),(1 1))");
    }

    #[tokio::test]
    async fn test_collection_extract_lines() {
        let ctx = SessionContext::new();
        ctx.register_udf(CollectionExtract::default().into());
        ctx.register_udf(GeomFromText::default().into());
        ctx.register_udf(AsText.into());

        let df = ctx
            .sql(
                "SELECT ST_AsText(ST_CollectionExtract(ST_GeomFromText('GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(1 1, 2 2))'), 2));",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch
            .column(0)
            .as_string::<i32>()
            .value(0);
        assert_eq!(val, "MULTILINESTRING((1 1,2 2))");
    }

    #[tokio::test]
    async fn test_collection_extract_default_highest_dim() {
        let ctx = SessionContext::new();
        ctx.register_udf(CollectionExtract::default().into());
        ctx.register_udf(GeomFromText::default().into());
        ctx.register_udf(AsText.into());

        // Without a type parameter, should extract highest-dimension type (polygon = 3)
        let df = ctx
            .sql(
                "SELECT ST_AsText(ST_CollectionExtract(ST_GeomFromText('GEOMETRYCOLLECTION(POINT(0 0), POLYGON((0 0, 1 0, 1 1, 0 1, 0 0)))')));",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let val = batch
            .column(0)
            .as_string::<i32>()
            .value(0);
        assert_eq!(val, "MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)))");
    }
}

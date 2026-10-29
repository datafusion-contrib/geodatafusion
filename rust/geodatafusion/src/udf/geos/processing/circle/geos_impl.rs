use std::sync::{Arc, OnceLock};

use arrow_array::builder::{Float64Builder, NullBufferBuilder};
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait, LineTrait,
    MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait, RectTrait,
    TriangleTrait,
};
use geoarrow_array::GeoArrowArray;
use geoarrow_array::builder::{GeometryBuilder, PointBuilder};
use geoarrow_schema::GeoArrowType;
use geos::sys;
use wkb::reader::{GeometryType as WkbGeometryType, Wkb};

use super::super::super::adapter;
use super::{
    geometry_wkb_column, null_record_array, record_array, record_fields, return_record_fields,
};
use crate::error::GeoDataFusionResult;

thread_local! {
    static GEOS_CONTEXT: RawGeosContext = RawGeosContext::new();
}

struct RawGeosContext {
    handle: sys::GEOSContextHandle_t,
}

impl RawGeosContext {
    fn new() -> Self {
        Self {
            handle: unsafe { sys::GEOS_init_r() },
        }
    }
}

impl Drop for RawGeosContext {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe { sys::GEOS_finish_r(self.handle) };
        }
    }
}

fn with_geos<T>(
    function_name: &str,
    operation: impl FnOnce(sys::GEOSContextHandle_t) -> GeoDataFusionResult<T>,
) -> GeoDataFusionResult<T> {
    GEOS_CONTEXT.with(|context| {
        if context.handle.is_null() {
            return Err(DataFusionError::Execution(format!(
                "{function_name} could not initialize a GEOS context"
            ))
            .into());
        }
        operation(context.handle)
    })
}

struct RawGeometry {
    handle: sys::GEOSContextHandle_t,
    pointer: *mut sys::GEOSGeometry,
}

impl RawGeometry {
    fn from_wkb(
        handle: sys::GEOSContextHandle_t,
        wkb: &[u8],
        function_name: &str,
    ) -> GeoDataFusionResult<Self> {
        let pointer = unsafe { sys::GEOSGeomFromWKB_buf_r(handle, wkb.as_ptr(), wkb.len()) };
        Self::new(handle, pointer, function_name, "decode input geometry")
    }

    fn new(
        handle: sys::GEOSContextHandle_t,
        pointer: *mut sys::GEOSGeometry,
        function_name: &str,
        operation: &str,
    ) -> GeoDataFusionResult<Self> {
        if pointer.is_null() {
            return Err(DataFusionError::Execution(format!(
                "{function_name} failed to {operation}"
            ))
            .into());
        }
        Ok(Self { handle, pointer })
    }

    fn as_ptr(&self) -> *const sys::GEOSGeometry {
        self.pointer
    }
}

impl Drop for RawGeometry {
    fn drop(&mut self) {
        if !self.pointer.is_null() {
            unsafe { sys::GEOSGeom_destroy_r(self.handle, self.pointer) };
        }
    }
}

fn geos_is_empty(
    handle: sys::GEOSContextHandle_t,
    geometry: &RawGeometry,
    operation: &str,
) -> GeoDataFusionResult<bool> {
    match unsafe { sys::GEOSisEmpty_r(handle, geometry.as_ptr()) } {
        0 => Ok(false),
        1 => Ok(true),
        2 => Err(DataFusionError::Execution(format!(
            "{operation}: GEOS isEmpty predicate raised an exception"
        ))
        .into()),
        status => Err(DataFusionError::Execution(format!(
            "{operation}: GEOS isEmpty predicate returned unexpected status {status}"
        ))
        .into()),
    }
}

fn geos_extent(
    handle: sys::GEOSContextHandle_t,
    geometry: &RawGeometry,
    function_name: &str,
) -> GeoDataFusionResult<[f64; 4]> {
    let mut xmin = 0.0;
    let mut ymin = 0.0;
    let mut xmax = 0.0;
    let mut ymax = 0.0;
    let result = unsafe {
        sys::GEOSGeom_getExtent_r(
            handle,
            geometry.as_ptr(),
            &mut xmin,
            &mut ymin,
            &mut xmax,
            &mut ymax,
        )
    };
    if result == 0 {
        return Err(DataFusionError::Execution(format!(
            "{function_name} failed to compute the input extent"
        ))
        .into());
    }
    Ok([xmin, ymin, xmax, ymax])
}

fn geos_coordinate(
    handle: sys::GEOSContextHandle_t,
    geometry: &RawGeometry,
    function_name: &str,
    operation: &str,
) -> GeoDataFusionResult<(f64, f64)> {
    let mut x = 0.0;
    let mut y = 0.0;
    let x_result = unsafe { sys::GEOSGeomGetX_r(handle, geometry.as_ptr(), &mut x) };
    let y_result = unsafe { sys::GEOSGeomGetY_r(handle, geometry.as_ptr(), &mut y) };
    if x_result == 0 || y_result == 0 {
        return Err(DataFusionError::Execution(format!(
            "{function_name} failed to read {operation}"
        ))
        .into());
    }
    Ok((x, y))
}

fn geos_distance(
    handle: sys::GEOSContextHandle_t,
    left: &RawGeometry,
    right: &RawGeometry,
    function_name: &str,
) -> GeoDataFusionResult<f64> {
    let mut distance = 0.0;
    let result =
        unsafe { sys::GEOSDistance_r(handle, left.as_ptr(), right.as_ptr(), &mut distance) };
    if result == 0 {
        return Err(DataFusionError::Execution(format!(
            "{function_name} failed to compute the center-to-nearest distance"
        ))
        .into());
    }
    Ok(distance)
}
fn auto_tolerance(extent: [f64; 4]) -> f64 {
    let width = extent[2] - extent[0];
    let height = extent[3] - extent[1];
    let size = if width > height { width } else { height };
    size / 1000.0
}

fn coordinates_are_finite(geometry: &impl GeometryTrait<T = f64>) -> bool {
    fn coord_is_finite(coord: &impl CoordTrait<T = f64>) -> bool {
        (0..coord.dim().size()).all(|index| coord.nth_or_panic(index).is_finite())
    }

    match geometry.as_type() {
        GeometryType::Point(point) => point.coord().is_none_or(|coord| coord_is_finite(&coord)),
        GeometryType::LineString(line_string) => {
            line_string.coords().all(|coord| coord_is_finite(&coord))
        }
        GeometryType::Polygon(polygon) => {
            polygon
                .exterior()
                .is_none_or(|ring| ring.coords().all(|coord| coord_is_finite(&coord)))
                && polygon
                    .interiors()
                    .all(|ring| ring.coords().all(|coord| coord_is_finite(&coord)))
        }
        GeometryType::MultiPoint(multi_point) => multi_point
            .points()
            .all(|point| point.coord().is_none_or(|coord| coord_is_finite(&coord))),
        GeometryType::MultiLineString(multi_line_string) => multi_line_string
            .line_strings()
            .all(|line_string| line_string.coords().all(|coord| coord_is_finite(&coord))),
        GeometryType::MultiPolygon(multi_polygon) => multi_polygon.polygons().all(|polygon| {
            polygon
                .exterior()
                .is_none_or(|ring| ring.coords().all(|coord| coord_is_finite(&coord)))
                && polygon
                    .interiors()
                    .all(|ring| ring.coords().all(|coord| coord_is_finite(&coord)))
        }),
        GeometryType::GeometryCollection(collection) => collection
            .geometries()
            .all(|child| coordinates_are_finite(&child)),
        GeometryType::Rect(rect) => coord_is_finite(&rect.min()) && coord_is_finite(&rect.max()),
        GeometryType::Triangle(triangle) => triangle.coords().iter().all(coord_is_finite),
        GeometryType::Line(line) => coord_is_finite(&line.start()) && coord_is_finite(&line.end()),
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct MaximumInscribedCircle {
    signature: Signature,
}

impl MaximumInscribedCircle {
    pub fn new() -> Self {
        Self {
            signature: adapter::signature(),
        }
    }
}

impl Default for MaximumInscribedCircle {
    fn default() -> Self {
        Self::new()
    }
}

static MAXIMUM_INSCRIBED_CIRCLE_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for MaximumInscribedCircle {
    fn name(&self) -> &str {
        "st_maximuminscribedcircle"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        adapter::check_arg_count(arg_types, &[1], self.name())?;
        adapter::check_geometry_type(&arg_types[0], 0, self.name())?;
        Ok(arg_types.to_vec())
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let (fields, _) = record_fields(&args, self.name(), true)?;
        Ok(Arc::new(Field::new("", DataType::Struct(fields), true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(maximum_inscribed_circle_impl(&args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(MAXIMUM_INSCRIBED_CIRCLE_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Computes the largest circle contained within a geometry. Polygonal inputs use GEOS MaximumInscribedCircle; point and linear inputs use GEOS LargestEmptyCircle with the input convex hull as the default boundary, matching PostGIS. Returns center, nearest, and radius; EMPTY input returns two empty POINTs and radius 0. The output is always 2D and keeps the input CRS.",
                "ST_MaximumInscribedCircle(geometry)",
            )
            .with_argument("geometry", "geometry")
            .build()
        }))
    }
}

fn maximum_inscribed_circle_impl(args: &ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let (fields, point_type) = return_record_fields(args)?;
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let row_count = arrays.first().map_or(0, Array::len);
    let Some(wkb) = geometry_wkb_column(&arrays, &args.arg_fields, 0, "ST_MaximumInscribedCircle")?
    else {
        return Ok(null_record_array(fields, point_type, row_count, true));
    };

    let mut center = PointBuilder::new(point_type.clone());
    let mut nearest = PointBuilder::new(point_type);
    let mut radius = Float64Builder::with_capacity(row_count);
    let mut validity = NullBufferBuilder::new(row_count);

    with_geos("ST_MaximumInscribedCircle", |handle| {
        for row in 0..row_count {
            if wkb.is_null(row) {
                center.push_null();
                nearest.push_null();
                radius.append_null();
                validity.append_null();
                continue;
            }

            let original = Wkb::try_new(wkb.value(row)).map_err(|error| {
                DataFusionError::Execution(format!(
                    "ST_MaximumInscribedCircle input WKB is invalid: {error}"
                ))
            })?;
            if !coordinates_are_finite(&original) {
                return Err(DataFusionError::Execution(
                    "ST_MaximumInscribedCircle geometry contains invalid coordinates".to_string(),
                )
                .into());
            }

            let prepared = adapter::prepare_geos_wkb(wkb.value(row))?;
            let parsed = Wkb::try_new(&prepared).map_err(|error| {
                DataFusionError::Execution(format!(
                    "ST_MaximumInscribedCircle prepared WKB is invalid: {error}"
                ))
            })?;
            let input = RawGeometry::from_wkb(handle, &prepared, "ST_MaximumInscribedCircle")?;
            if geos_is_empty(handle, &input, "ST_MaximumInscribedCircle")? {
                center.push_empty();
                nearest.push_empty();
                radius.append_value(0.0);
                validity.append_non_null();
                continue;
            }

            let tolerance =
                auto_tolerance(geos_extent(handle, &input, "ST_MaximumInscribedCircle")?);
            let circle_pointer = if matches!(
                parsed.geometry_type(),
                WkbGeometryType::Polygon | WkbGeometryType::MultiPolygon
            ) {
                unsafe { sys::GEOSMaximumInscribedCircle_r(handle, input.as_ptr(), tolerance) }
            } else {
                unsafe {
                    sys::GEOSLargestEmptyCircle_r(
                        handle,
                        input.as_ptr(),
                        std::ptr::null(),
                        tolerance,
                    )
                }
            };
            let circle = RawGeometry::new(
                handle,
                circle_pointer,
                "ST_MaximumInscribedCircle",
                "compute the inscribed circle",
            )?;
            let center_geometry = RawGeometry::new(
                handle,
                unsafe { sys::GEOSGeomGetStartPoint_r(handle, circle.as_ptr()) },
                "ST_MaximumInscribedCircle",
                "read the center point",
            )?;
            let nearest_geometry = RawGeometry::new(
                handle,
                unsafe { sys::GEOSGeomGetEndPoint_r(handle, circle.as_ptr()) },
                "ST_MaximumInscribedCircle",
                "read the nearest point",
            )?;
            let (center_x, center_y) = geos_coordinate(
                handle,
                &center_geometry,
                "ST_MaximumInscribedCircle",
                "the center point",
            )?;
            let (nearest_x, nearest_y) = geos_coordinate(
                handle,
                &nearest_geometry,
                "ST_MaximumInscribedCircle",
                "the nearest point",
            )?;
            let distance = geos_distance(
                handle,
                &center_geometry,
                &nearest_geometry,
                "ST_MaximumInscribedCircle",
            )?;

            center.push_coord(Some(&geo::Coord::<f64> {
                x: center_x,
                y: center_y,
            }));
            nearest.push_coord(Some(&geo::Coord::<f64> {
                x: nearest_x,
                y: nearest_y,
            }));
            radius.append_value(distance);
            validity.append_non_null();
        }
        Ok(())
    })?;

    Ok(record_array(
        fields,
        center,
        Some(nearest),
        radius,
        validity,
    ))
}

fn geos_minimum_bounding_circle(
    handle: sys::GEOSContextHandle_t,
    input: &RawGeometry,
    function_name: &str,
) -> GeoDataFusionResult<(f64, f64, f64)> {
    let mut radius = 0.0;
    let mut center_pointer = std::ptr::null_mut();
    let circle_pointer = unsafe {
        sys::GEOSMinimumBoundingCircle_r(handle, input.as_ptr(), &mut radius, &mut center_pointer)
    };
    let circle = RawGeometry::new(
        handle,
        circle_pointer,
        function_name,
        "compute the minimum bounding circle",
    )?;
    let center = RawGeometry::new(
        handle,
        center_pointer,
        function_name,
        "read the minimum bounding circle center",
    )?;
    let (center_x, center_y) = geos_coordinate(
        handle,
        &center,
        function_name,
        "the minimum bounding circle center",
    )?;
    drop(circle);
    drop(center);
    Ok((center_x, center_y, radius))
}

fn point_wkb(x: f64, y: f64) -> Vec<u8> {
    let mut output = Vec::with_capacity(21);
    output.push(1);
    output.extend_from_slice(&1_u32.to_le_bytes());
    output.extend_from_slice(&x.to_le_bytes());
    output.extend_from_slice(&y.to_le_bytes());
    output
}

fn circle_polygon_wkb(
    center_x: f64,
    center_y: f64,
    radius: f64,
    segments_per_quarter: i64,
) -> Vec<u8> {
    let segments = segments_per_quarter * 4;
    let theta = 2.0 * std::f64::consts::PI / segments as f64;
    // Place vertices on the circumscribed polygon whose chord midpoints lie on
    // the requested circle: r_vertex = r / cos(theta / 2).
    let exterior_radius = radius / (theta / 2.0).cos();

    let mut output = Vec::with_capacity(1 + 4 + 4 + 4 + (segments as usize + 1) * 16);
    output.push(1);
    output.extend_from_slice(&3_u32.to_le_bytes());
    output.extend_from_slice(&1_u32.to_le_bytes());
    output.extend_from_slice(&((segments + 1) as u32).to_le_bytes());
    for index in 0..=segments {
        let angle = index as f64 * theta;
        let x = center_x + exterior_radius * angle.sin();
        let y = center_y + exterior_radius * angle.cos();
        output.extend_from_slice(&x.to_le_bytes());
        output.extend_from_slice(&y.to_le_bytes());
    }
    output
}

fn geometry_array_from_wkb<'a>(
    values: impl Iterator<Item = Option<&'a [u8]>>,
    output_type: &GeoArrowType,
) -> GeoDataFusionResult<ArrayRef> {
    let GeoArrowType::Geometry(geometry_type) = output_type else {
        return Err(DataFusionError::Internal(
            "ST_MinimumBoundingCircle result field is not a generic Geometry".to_string(),
        )
        .into());
    };
    let mut builder = GeometryBuilder::new(geometry_type.clone());
    for value in values {
        match value {
            Some(wkb) => {
                let geometry = Wkb::try_new(wkb).map_err(|error| {
                    DataFusionError::Execution(format!(
                        "ST_MinimumBoundingCircle result WKB is invalid: {error}"
                    ))
                })?;
                builder.push_geometry(Some(&geometry))?;
            }
            None => {
                builder.push_geometry(None::<&geo_traits::UnimplementedGeometry<f64>>)?;
            }
        }
    }
    Ok(builder.finish().into_array_ref())
}

fn minimum_bounding_geometry_impl(
    args: &ScalarFunctionArgs,
    function_name: &str,
    is_circle: bool,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let row_count = arrays.first().map_or(0, Array::len);
    let Some(wkb) = geometry_wkb_column(&arrays, &args.arg_fields, 0, function_name)? else {
        if is_circle {
            let output_geometry_type = GeoArrowType::from_arrow_field(args.return_field.as_ref())?;
            let values = std::iter::repeat_n(None, row_count);
            return Ok(ColumnarValue::Array(geometry_array_from_wkb(
                values,
                &output_geometry_type,
            )?));
        }
        let (fields, point_type) = return_record_fields(args)?;
        return Ok(null_record_array(fields, point_type, row_count, false));
    };

    let mut geometry_values: Vec<Option<Vec<u8>>> = if is_circle {
        Vec::with_capacity(row_count)
    } else {
        Vec::new()
    };
    let mut radius_values: Vec<Option<f64>> = Vec::with_capacity(row_count);
    let mut center_values: Vec<Option<(f64, f64)>> = Vec::with_capacity(row_count);

    with_geos(function_name, |handle| {
        for row in 0..row_count {
            if wkb.is_null(row) {
                if is_circle {
                    geometry_values.push(None);
                }
                radius_values.push(None);
                center_values.push(None);
                continue;
            }
            let segments_per_quarter = if is_circle && args.args.len() == 2 {
                match adapter::int64_value(&arrays, 1, row, function_name)? {
                    Some(value) => Some(value),
                    None => {
                        geometry_values.push(None);
                        radius_values.push(None);
                        center_values.push(None);
                        continue;
                    }
                }
            } else {
                None
            };

            let prepared = adapter::prepare_geos_wkb(wkb.value(row))?;
            let input = RawGeometry::from_wkb(handle, &prepared, function_name)?;
            if geos_is_empty(handle, &input, function_name)? {
                if is_circle {
                    geometry_values.push(Some(EMPTY_POINT_WKB.to_vec()));
                }
                radius_values.push(Some(0.0));
                center_values.push(None);
                continue;
            }

            let extent = geos_extent(handle, &input, function_name)?;
            let (center_x, center_y, radius) = if extent[0] == extent[2] && extent[1] == extent[3] {
                // GEOSMinimumBoundingCircle returns an empty center when
                // multiple input vertices collapse to one convex-hull
                // point. PostGIS defines that degenerate case as a
                // zero-radius circle at the shared coordinate.
                (extent[0], extent[1], 0.0)
            } else {
                geos_minimum_bounding_circle(handle, &input, function_name)?
            };
            if radius == 0.0 {
                if is_circle {
                    geometry_values.push(Some(point_wkb(center_x, center_y)));
                }
                radius_values.push(Some(0.0));
                center_values.push(Some((center_x, center_y)));
                continue;
            }

            if is_circle {
                let segments_per_quarter =
                    segments_per_quarter.unwrap_or(DEFAULT_SEGMENTS_PER_QUARTER);
                if segments_per_quarter <= 0 || segments_per_quarter > i32::MAX as i64 / 4 {
                    return Err(DataFusionError::Execution(
                        "ST_MinimumBoundingCircle needs between 1 and 536870911 segments per quarter-circle"
                            .to_string(),
                    )
                    .into());
                }
                geometry_values.push(Some(circle_polygon_wkb(
                    center_x,
                    center_y,
                    radius,
                    segments_per_quarter,
                )));
            }
            radius_values.push(Some(radius));
            center_values.push(Some((center_x, center_y)));
        }
        Ok(())
    })?;

    if is_circle {
        let output_geometry_type = GeoArrowType::from_arrow_field(args.return_field.as_ref())?;
        return Ok(ColumnarValue::Array(geometry_array_from_wkb(
            geometry_values.iter().map(|value| value.as_deref()),
            &output_geometry_type,
        )?));
    }

    let (fields, point_type) = return_record_fields(args)?;
    let mut center = PointBuilder::new(point_type);
    let mut radius = Float64Builder::with_capacity(row_count);
    let mut validity = NullBufferBuilder::new(row_count);
    for row in 0..row_count {
        match center_values[row] {
            Some((x, y)) => {
                center.push_coord(Some(&geo::Coord::<f64> { x, y }));
                radius.append_value(radius_values[row].unwrap_or_default());
                validity.append_non_null();
            }
            None if radius_values[row].is_some() => {
                center.push_empty();
                radius.append_value(0.0);
                validity.append_non_null();
            }
            None => {
                center.push_null();
                radius.append_null();
                validity.append_null();
            }
        }
    }
    Ok(record_array(fields, center, None, radius, validity))
}

const DEFAULT_SEGMENTS_PER_QUARTER: i64 = 48;
const EMPTY_POINT_WKB: &[u8] = &[
    1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xf8, 0x7f, 0, 0, 0, 0, 0, 0, 0xf8, 0x7f,
];

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct MinimumBoundingRadius {
    signature: Signature,
}

impl MinimumBoundingRadius {
    pub fn new() -> Self {
        Self {
            signature: adapter::signature(),
        }
    }
}

impl Default for MinimumBoundingRadius {
    fn default() -> Self {
        Self::new()
    }
}

static MINIMUM_BOUNDING_RADIUS_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for MinimumBoundingRadius {
    fn name(&self) -> &str {
        "st_minimumboundingradius"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        adapter::check_arg_count(arg_types, &[1], self.name())?;
        adapter::check_geometry_type(&arg_types[0], 0, self.name())?;
        Ok(arg_types.to_vec())
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let (fields, _) = record_fields(&args, self.name(), false)?;
        Ok(Arc::new(Field::new("", DataType::Struct(fields), true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(minimum_bounding_geometry_impl(
            &args,
            "ST_MinimumBoundingRadius",
            false,
        )?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(MINIMUM_BOUNDING_RADIUS_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Computes the center and radius of the smallest enclosing circle with GEOSMinimumBoundingCircle. EMPTY input returns an empty center POINT and radius 0. The output is always 2D and keeps the input CRS.",
                "ST_MinimumBoundingRadius(geometry)",
            )
            .with_argument("geometry", "geometry")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct MinimumBoundingCircle {
    signature: Signature,
}

impl MinimumBoundingCircle {
    pub fn new() -> Self {
        Self {
            signature: adapter::signature(),
        }
    }
}

impl Default for MinimumBoundingCircle {
    fn default() -> Self {
        Self::new()
    }
}

static MINIMUM_BOUNDING_CIRCLE_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for MinimumBoundingCircle {
    fn name(&self) -> &str {
        "st_minimumboundingcircle"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        adapter::check_arg_count(arg_types, &[1, 2], self.name())?;
        adapter::check_geometry_type(&arg_types[0], 0, self.name())?;
        let mut coerced = arg_types.to_vec();
        if arg_types.len() == 2 {
            coerced[1] = adapter::coerce_int64(&arg_types[1], 1, self.name())?;
        }
        Ok(coerced)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        adapter::geometry_return_field(&args, &[0], self.name())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(minimum_bounding_geometry_impl(
            &args,
            "ST_MinimumBoundingCircle",
            true,
        )?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(MINIMUM_BOUNDING_CIRCLE_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a polygon approximation of the circle from GEOSMinimumBoundingCircle. segs_per_quarter controls the number of segments in each quarter-circle and defaults to 48. Vertices are placed on the circle at equal angles so the polygon's edge midpoints lie on the requested circle; zero-radius input returns a POINT, EMPTY returns POINT EMPTY, and the output is always 2D with the input CRS.",
                "ST_MinimumBoundingCircle(geometry, segs_per_quarter)",
            )
            .with_argument("geometry", "geometry")
            .with_argument("segs_per_quarter", "optional positive integer")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct LargestEmptyCircle {
    signature: Signature,
}

impl LargestEmptyCircle {
    pub fn new() -> Self {
        Self {
            signature: adapter::signature(),
        }
    }
}

impl Default for LargestEmptyCircle {
    fn default() -> Self {
        Self::new()
    }
}

static LARGEST_EMPTY_CIRCLE_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for LargestEmptyCircle {
    fn name(&self) -> &str {
        "st_largestemptycircle"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        adapter::check_arg_count(arg_types, &[1, 2, 3], self.name())?;
        adapter::check_geometry_type(&arg_types[0], 0, self.name())?;
        let mut coerced = arg_types.to_vec();
        if arg_types.len() >= 2 {
            coerced[1] = adapter::coerce_float64(&arg_types[1], 1, self.name())?;
        }
        if arg_types.len() == 3 {
            adapter::check_geometry_type(&arg_types[2], 2, self.name())?;
        }
        Ok(coerced)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let (fields, _) = record_fields(&args, self.name(), true)?;
        Ok(Arc::new(Field::new("", DataType::Struct(fields), true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(largest_empty_circle_impl(&args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(LARGEST_EMPTY_CIRCLE_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Computes the largest circle that does not overlap a set of point, line, or polygon-boundary obstacles. The optional boundary constrains the circle center; an EMPTY or omitted boundary uses the convex hull of the input. A non-positive tolerance selects the same default tolerance as PostGIS. Returns center, nearest, and radius, is strict on NULL arguments, and always returns 2D output with the input CRS.",
                "ST_LargestEmptyCircle(geometry, tolerance, boundary)",
            )
            .with_argument("geometry", "obstacle geometry")
            .with_argument("tolerance", "optional positive distance tolerance")
            .with_argument("boundary", "optional polygonal boundary")
            .build()
        }))
    }
}

fn largest_empty_circle_impl(args: &ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let (fields, point_type) = return_record_fields(args)?;
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let row_count = arrays.first().map_or(0, Array::len);
    let Some(wkb) = geometry_wkb_column(&arrays, &args.arg_fields, 0, "ST_LargestEmptyCircle")?
    else {
        return Ok(null_record_array(fields, point_type, row_count, true));
    };
    let boundary = if args.args.len() == 3 {
        geometry_wkb_column(&arrays, &args.arg_fields, 2, "ST_LargestEmptyCircle")?
    } else {
        None
    };
    if args.args.len() == 3 && boundary.is_none() {
        return Ok(null_record_array(fields, point_type, row_count, true));
    }

    let mut center = PointBuilder::new(point_type.clone());
    let mut nearest = PointBuilder::new(point_type);
    let mut radius = Float64Builder::with_capacity(row_count);
    let mut validity = NullBufferBuilder::new(row_count);

    with_geos("ST_LargestEmptyCircle", |handle| {
        for row in 0..row_count {
            if wkb.is_null(row) || boundary.as_ref().is_some_and(|values| values.is_null(row)) {
                center.push_null();
                nearest.push_null();
                radius.append_null();
                validity.append_null();
                continue;
            }

            let tolerance = if args.args.len() >= 2 {
                match adapter::float64_value(&arrays, 1, row, "ST_LargestEmptyCircle")? {
                    Some(value) => value,
                    None => {
                        center.push_null();
                        nearest.push_null();
                        radius.append_null();
                        validity.append_null();
                        continue;
                    }
                }
            } else {
                0.0
            };

            let original = Wkb::try_new(wkb.value(row)).map_err(|error| {
                DataFusionError::Execution(format!(
                    "ST_LargestEmptyCircle input WKB is invalid: {error}"
                ))
            })?;
            if !coordinates_are_finite(&original) {
                return Err(DataFusionError::Execution(
                    "ST_LargestEmptyCircle geometry contains invalid coordinates".to_string(),
                )
                .into());
            }
            let prepared = adapter::prepare_geos_wkb(wkb.value(row))?;
            let input = RawGeometry::from_wkb(handle, &prepared, "ST_LargestEmptyCircle")?;
            if geos_is_empty(handle, &input, "ST_LargestEmptyCircle")? {
                center.push_empty();
                nearest.push_empty();
                radius.append_value(0.0);
                validity.append_non_null();
                continue;
            }

            let boundary_geometry = match boundary.as_ref() {
                Some(values) => {
                    let boundary_wkb = values.value(row);
                    let parsed_boundary = Wkb::try_new(boundary_wkb).map_err(|error| {
                        DataFusionError::Execution(format!(
                            "ST_LargestEmptyCircle boundary WKB is invalid: {error}"
                        ))
                    })?;
                    if !coordinates_are_finite(&parsed_boundary) {
                        return Err(DataFusionError::Execution(
                            "ST_LargestEmptyCircle boundary contains invalid coordinates"
                                .to_string(),
                        )
                        .into());
                    }
                    let prepared_boundary = adapter::prepare_geos_wkb(boundary_wkb)?;
                    let geometry =
                        RawGeometry::from_wkb(handle, &prepared_boundary, "ST_LargestEmptyCircle")?;
                    if geos_is_empty(handle, &geometry, "ST_LargestEmptyCircle boundary")? {
                        None
                    } else {
                        Some(geometry)
                    }
                }
                None => None,
            };

            let tolerance = if tolerance <= 0.0 {
                auto_tolerance(geos_extent(handle, &input, "ST_LargestEmptyCircle")?)
            } else {
                tolerance
            };
            let boundary_pointer = boundary_geometry
                .as_ref()
                .map_or(std::ptr::null(), |geometry| geometry.as_ptr());
            let circle = RawGeometry::new(
                handle,
                unsafe {
                    sys::GEOSLargestEmptyCircle_r(
                        handle,
                        input.as_ptr(),
                        boundary_pointer,
                        tolerance,
                    )
                },
                "ST_LargestEmptyCircle",
                "compute the empty circle",
            )?;
            let center_geometry = RawGeometry::new(
                handle,
                unsafe { sys::GEOSGeomGetStartPoint_r(handle, circle.as_ptr()) },
                "ST_LargestEmptyCircle",
                "read the center point",
            )?;
            let nearest_geometry = RawGeometry::new(
                handle,
                unsafe { sys::GEOSGeomGetEndPoint_r(handle, circle.as_ptr()) },
                "ST_LargestEmptyCircle",
                "read the nearest point",
            )?;
            let (center_x, center_y) = geos_coordinate(
                handle,
                &center_geometry,
                "ST_LargestEmptyCircle",
                "the center point",
            )?;
            let (nearest_x, nearest_y) = geos_coordinate(
                handle,
                &nearest_geometry,
                "ST_LargestEmptyCircle",
                "the nearest point",
            )?;
            let distance = geos_distance(
                handle,
                &center_geometry,
                &nearest_geometry,
                "ST_LargestEmptyCircle",
            )?;

            center.push_coord(Some(&geo::Coord::<f64> {
                x: center_x,
                y: center_y,
            }));
            nearest.push_coord(Some(&geo::Coord::<f64> {
                x: nearest_x,
                y: nearest_y,
            }));
            radius.append_value(distance);
            validity.append_non_null();
        }
        Ok(())
    })?;

    Ok(record_array(
        fields,
        center,
        Some(nearest),
        radius,
        validity,
    ))
}

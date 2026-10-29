//! Small reentrant GEOS C API helpers for operations that the safe `geos`
//! bindings do not expose. Each thread owns one context so calls stay
//! reentrant without sharing GEOS handles across threads.

use std::ffi::CStr;
use std::os::raw::c_char;
use std::slice;

use datafusion::error::DataFusionError;
use geos::sys;

use crate::error::GeoDataFusionResult;

thread_local! {
    static CONTEXT: RawContext = RawContext::new();
}

struct RawContext {
    handle: sys::GEOSContextHandle_t,
}

impl RawContext {
    fn new() -> Self {
        Self {
            handle: unsafe { sys::GEOS_init_r() },
        }
    }
}

impl Drop for RawContext {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe { sys::GEOS_finish_r(self.handle) };
        }
    }
}

pub(super) fn with_context<R>(
    operation: &str,
    f: impl FnOnce(sys::GEOSContextHandle_t) -> GeoDataFusionResult<R>,
) -> GeoDataFusionResult<R> {
    CONTEXT.with(|context| {
        if context.handle.is_null() {
            return Err(DataFusionError::Execution(format!(
                "{operation}: GEOS context initialization failed"
            ))
            .into());
        }
        f(context.handle)
    })
}

pub(super) unsafe fn read_wkb(
    handle: sys::GEOSContextHandle_t,
    wkb: &[u8],
    operation: &str,
) -> GeoDataFusionResult<*mut sys::GEOSGeometry> {
    let geometry = unsafe { sys::GEOSGeomFromWKB_buf_r(handle, wkb.as_ptr(), wkb.len()) };
    if geometry.is_null() {
        return Err(DataFusionError::Execution(format!(
            "{operation}: GEOS could not read WKB input"
        ))
        .into());
    }
    Ok(geometry)
}

pub(super) unsafe fn write_wkb(
    handle: sys::GEOSContextHandle_t,
    geometry: *const sys::GEOSGeometry,
    operation: &str,
) -> GeoDataFusionResult<Vec<u8>> {
    let writer = unsafe { sys::GEOSWKBWriter_create_r(handle) };
    if writer.is_null() {
        return Err(DataFusionError::Execution(format!(
            "{operation}: GEOS could not create a WKB writer"
        ))
        .into());
    }

    let dimension = unsafe { sys::GEOSGeom_getCoordinateDimension_r(handle, geometry) };
    let dimension = if dimension > 2 { 3 } else { 2 };
    unsafe { sys::GEOSWKBWriter_setOutputDimension_r(handle, writer, dimension) };

    let mut size = 0;
    let buffer = unsafe { sys::GEOSWKBWriter_write_r(handle, writer, geometry, &mut size) };
    unsafe { sys::GEOSWKBWriter_destroy_r(handle, writer) };
    if buffer.is_null() {
        return Err(DataFusionError::Execution(format!(
            "{operation}: GEOS could not encode its result"
        ))
        .into());
    }

    let bytes = unsafe { slice::from_raw_parts(buffer, size) }.to_vec();
    unsafe { sys::GEOSFree_r(handle, buffer.cast()) };
    Ok(bytes)
}

pub(super) unsafe fn destroy_geometry(
    handle: sys::GEOSContextHandle_t,
    geometry: *mut sys::GEOSGeometry,
) {
    if !geometry.is_null() {
        unsafe { sys::GEOSGeom_destroy_r(handle, geometry) };
    }
}

pub(super) unsafe fn take_string(
    handle: sys::GEOSContextHandle_t,
    value: *mut c_char,
    operation: &str,
) -> GeoDataFusionResult<String> {
    if value.is_null() {
        return Err(DataFusionError::Execution(format!(
            "{operation}: GEOS returned an unexpected null string"
        ))
        .into());
    }
    let text = unsafe { CStr::from_ptr(value) }
        .to_string_lossy()
        .into_owned();
    unsafe { sys::GEOSFree_r(handle, value.cast()) };
    Ok(text)
}

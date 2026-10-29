//! Minimal ownership-safe wrappers around the GEOS coverage C API.
//!
//! The `geos` crate safely wraps `GEOSCoverageUnion_r` but does not expose the
//! GEOS 3.12 `GEOSCoverageIsValid_r` and `GEOSCoverageSimplifyVW_r` functions.
//! This module owns one GEOS context per calculation and releases every GEOS
//! object through a guard, including on error paths.

use std::cell::RefCell;
use std::ffi::{CStr, c_char, c_int, c_uint, c_void};
use std::ptr;

use datafusion::error::DataFusionError;
use geos::sys;

use crate::error::GeoDataFusionResult;

thread_local! {
    static LAST_ERROR: RefCell<Option<String>> = const { RefCell::new(None) };
}

unsafe extern "C" fn error_handler(message: *const c_char, _userdata: *mut c_void) {
    if message.is_null() {
        return;
    }
    let message = unsafe { CStr::from_ptr(message) }
        .to_string_lossy()
        .into_owned();
    LAST_ERROR.with(|last| {
        *last.borrow_mut() = Some(message);
    });
}

fn geos_error(operation: &str, fallback: &str) -> DataFusionError {
    let message = LAST_ERROR.with(|last| last.borrow_mut().take());
    DataFusionError::Execution(match message {
        Some(message) => format!("{operation}: {message}"),
        None => format!("{operation}: {fallback}"),
    })
}

#[cfg(feature = "geos-3_12")]
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum WindowOperation {
    InvalidEdges {
        tolerance: f64,
    },
    Simplify {
        tolerance: f64,
        simplify_boundary: bool,
    },
}

struct Handle {
    context: sys::GEOSContextHandle_t,
    reader: *mut sys::GEOSWKBReader,
    writer: *mut sys::GEOSWKBWriter,
    collection: *mut sys::GEOSGeometry,
    result: *mut sys::GEOSGeometry,
    orphans: Vec<*mut sys::GEOSGeometry>,
}

impl Handle {
    fn new(operation: &str) -> GeoDataFusionResult<Self> {
        unsafe {
            let context = sys::GEOS_init_r();
            if context.is_null() {
                return Err(geos_error(operation, "failed to initialize GEOS").into());
            }
            sys::GEOSContext_setErrorMessageHandler_r(
                context,
                Some(error_handler),
                ptr::null_mut(),
            );
            LAST_ERROR.with(|last| {
                *last.borrow_mut() = None;
            });

            let reader = sys::GEOSWKBReader_create_r(context);
            if reader.is_null() {
                sys::GEOS_finish_r(context);
                return Err(geos_error(operation, "failed to create a WKB reader").into());
            }
            let writer = sys::GEOSWKBWriter_create_r(context);
            if writer.is_null() {
                sys::GEOSWKBReader_destroy_r(context, reader);
                sys::GEOS_finish_r(context);
                return Err(geos_error(operation, "failed to create a WKB writer").into());
            }

            Ok(Self {
                context,
                reader,
                writer,
                collection: ptr::null_mut(),
                result: ptr::null_mut(),
                orphans: Vec::new(),
            })
        }
    }

    fn read(&mut self, wkb: &[u8], operation: &str) -> GeoDataFusionResult<*mut sys::GEOSGeometry> {
        let geometry = unsafe {
            sys::GEOSWKBReader_read_r(self.context, self.reader, wkb.as_ptr(), wkb.len())
        };
        if geometry.is_null() {
            Err(geos_error(operation, "failed to decode input WKB").into())
        } else {
            Ok(geometry)
        }
    }

    fn is_empty(
        &self,
        geometry: *const sys::GEOSGeometry,
        operation: &str,
    ) -> GeoDataFusionResult<bool> {
        predicate(
            unsafe { sys::GEOSisEmpty_r(self.context, geometry) },
            operation,
            "failed to test whether a geometry is empty",
        )
    }

    fn is_polygonal(
        &self,
        geometry: *const sys::GEOSGeometry,
        operation: &str,
    ) -> GeoDataFusionResult<bool> {
        let geometry_type = unsafe { sys::GEOSGeomTypeId_r(self.context, geometry) };
        if geometry_type < 0 {
            return Err(geos_error(operation, "failed to read a geometry type").into());
        }
        Ok(matches!(
            geometry_type,
            value if value == sys::GEOSGeomTypes_GEOS_POLYGON as c_int
                || value == sys::GEOSGeomTypes_GEOS_MULTIPOLYGON as c_int
        ))
    }

    fn create_collection(
        &mut self,
        operation: &str,
    ) -> GeoDataFusionResult<*mut sys::GEOSGeometry> {
        if self.orphans.is_empty() {
            return Ok(ptr::null_mut());
        }
        let collection = unsafe {
            sys::GEOSGeom_createCollection_r(
                self.context,
                sys::GEOSGeomTypes_GEOS_GEOMETRYCOLLECTION as c_int,
                self.orphans.as_mut_ptr(),
                self.orphans.len() as c_uint,
            )
        };
        if collection.is_null() {
            return Err(geos_error(operation, "failed to create a geometry collection").into());
        }
        // The collection owns its members from this point on.
        self.orphans.clear();
        Ok(collection)
    }

    #[cfg(feature = "geos-3_12")]
    fn write(
        &mut self,
        geometry: *const sys::GEOSGeometry,
        operation: &str,
    ) -> GeoDataFusionResult<Vec<u8>> {
        let dimension = unsafe { sys::GEOSGeom_getCoordinateDimension_r(self.context, geometry) };
        if !(2..=3).contains(&dimension) {
            return Err(
                geos_error(operation, "GEOS returned a geometry without XY coordinates").into(),
            );
        }
        self.write_dimension(geometry, dimension, operation)
    }

    fn write_dimension(
        &mut self,
        geometry: *const sys::GEOSGeometry,
        dimension: c_int,
        operation: &str,
    ) -> GeoDataFusionResult<Vec<u8>> {
        unsafe {
            sys::GEOSWKBWriter_setOutputDimension_r(self.context, self.writer, dimension);
        }

        let mut size = 0usize;
        let buffer =
            unsafe { sys::GEOSWKBWriter_write_r(self.context, self.writer, geometry, &mut size) };
        if buffer.is_null() {
            return Err(geos_error(operation, "failed to encode a GEOS result").into());
        }
        let bytes = unsafe { std::slice::from_raw_parts(buffer, size) }.to_vec();
        unsafe {
            sys::GEOSFree_r(self.context, buffer.cast());
        }
        Ok(bytes)
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        unsafe {
            destroy(self.context, self.result);
            destroy(self.context, self.collection);
            for geometry in self.orphans.drain(..) {
                destroy(self.context, geometry);
            }
            if !self.writer.is_null() {
                sys::GEOSWKBWriter_destroy_r(self.context, self.writer);
            }
            if !self.reader.is_null() {
                sys::GEOSWKBReader_destroy_r(self.context, self.reader);
            }
            sys::GEOS_finish_r(self.context);
        }
    }
}

unsafe fn destroy(context: sys::GEOSContextHandle_t, geometry: *mut sys::GEOSGeometry) {
    if !geometry.is_null() {
        unsafe { sys::GEOSGeom_destroy_r(context, geometry) };
    }
}

fn predicate(value: c_char, operation: &str, fallback: &str) -> GeoDataFusionResult<bool> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(geos_error(operation, fallback).into()),
    }
}

fn read_participants(
    handle: &mut Handle,
    inputs: &[Option<Vec<u8>>],
    operation: &str,
    require_polygonal: bool,
) -> GeoDataFusionResult<(Vec<Option<usize>>, usize)> {
    let mut mapping = vec![None; inputs.len()];
    let mut count = 0usize;

    for (position, input) in inputs.iter().enumerate() {
        let Some(wkb) = input else {
            continue;
        };
        let prepared = super::super::adapter::prepare_geos_wkb(wkb)?;
        let geometry = handle.read(prepared.as_ref(), operation)?;

        let accepted = if handle.is_empty(geometry, operation)? {
            false
        } else if require_polygonal {
            handle.is_polygonal(geometry, operation)?
        } else {
            true
        };
        if !accepted {
            unsafe { destroy(handle.context, geometry) };
            continue;
        }

        mapping[position] = Some(count);
        count += 1;
        handle.orphans.push(geometry);
    }

    if count > 0 {
        let collection = handle.create_collection(operation)?;
        handle.collection = collection;
    }
    Ok((mapping, count))
}

#[cfg(feature = "geos-3_12")]
fn read_collection_member(
    handle: &Handle,
    collection: *const sys::GEOSGeometry,
    index: usize,
    operation: &str,
) -> GeoDataFusionResult<*const sys::GEOSGeometry> {
    let geometry = unsafe { sys::GEOSGetGeometryN_r(handle.context, collection, index as c_int) };
    if geometry.is_null() {
        Err(geos_error(operation, "GEOS returned a shorter result collection").into())
    } else {
        Ok(geometry)
    }
}

#[cfg(feature = "geos-3_12")]
fn result_length(
    handle: &Handle,
    collection: *const sys::GEOSGeometry,
    operation: &str,
) -> GeoDataFusionResult<usize> {
    let length = unsafe { sys::GEOSGetNumGeometries_r(handle.context, collection) };
    if length < 0 {
        Err(geos_error(operation, "failed to read the GEOS result collection").into())
    } else {
        Ok(length as usize)
    }
}

#[cfg(feature = "geos-3_12")]
pub(super) fn coverage_window(
    inputs: &[Option<Vec<u8>>],
    operation: WindowOperation,
) -> GeoDataFusionResult<Vec<Option<Vec<u8>>>> {
    let function = match operation {
        WindowOperation::InvalidEdges { .. } => "ST_CoverageInvalidEdges",
        WindowOperation::Simplify { .. } => "ST_CoverageSimplify",
    };
    let mut handle = Handle::new(function)?;
    let (mapping, participant_count) = read_participants(&mut handle, inputs, function, true)?;
    if participant_count == 0 {
        return Ok(vec![None; inputs.len()]);
    }

    let result = match operation {
        WindowOperation::InvalidEdges { tolerance } => {
            let mut result = ptr::null_mut();
            let code = unsafe {
                sys::GEOSCoverageIsValid_r(
                    handle.context,
                    handle.collection,
                    tolerance,
                    &mut result,
                )
            };
            if code == 2 || result.is_null() {
                return Err(geos_error(function, "coverage validation failed").into());
            }
            result
        }
        WindowOperation::Simplify {
            tolerance,
            simplify_boundary,
        } => {
            let preserve_boundary = c_int::from(!simplify_boundary);
            let result = unsafe {
                sys::GEOSCoverageSimplifyVW_r(
                    handle.context,
                    handle.collection,
                    tolerance,
                    preserve_boundary,
                )
            };
            if result.is_null() {
                return Err(geos_error(function, "coverage simplification failed").into());
            }
            result
        }
    };
    handle.result = result;

    if result_length(&handle, result, function)? != participant_count {
        return Err(DataFusionError::Internal(format!(
            "{function}: GEOS returned a different number of geometries than the coverage input"
        ))
        .into());
    }

    let invalid_edges = matches!(operation, WindowOperation::InvalidEdges { .. });
    let mut outputs = vec![None; inputs.len()];
    for (position, participant) in mapping.iter().enumerate() {
        let Some(index) = participant else {
            continue;
        };
        let geometry = read_collection_member(&handle, result, *index, function)?;
        if invalid_edges && handle.is_empty(geometry, function)? {
            continue;
        }
        outputs[position] = Some(handle.write(geometry, function)?);
    }
    Ok(outputs)
}

pub(super) fn coverage_union(inputs: &[Option<Vec<u8>>]) -> GeoDataFusionResult<Option<Vec<u8>>> {
    const FUNCTION: &str = "ST_CoverageUnion";
    let mut handle = Handle::new(FUNCTION)?;
    let (_, participant_count) = read_participants(&mut handle, inputs, FUNCTION, false)?;
    if participant_count == 0 {
        return Ok(None);
    }

    let result = unsafe { sys::GEOSCoverageUnion_r(handle.context, handle.collection) };
    if result.is_null() {
        return Err(geos_error(FUNCTION, "coverage union failed").into());
    }
    handle.result = result;
    // PostGIS 3.6 with GEOS 3.14 returns a 2D coverage union even when the
    // inputs contain Z. GEOS 3.15 started preserving Z, so request XY output
    // explicitly to keep the SQL contract stable across supported GEOS
    // versions.
    Ok(Some(handle.write_dimension(result, 2, FUNCTION)?))
}

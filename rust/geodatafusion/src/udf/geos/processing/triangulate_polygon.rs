use std::slice;
use std::sync::OnceLock;

use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo_traits::{GeometryCollectionTrait, GeometryTrait};
use geos::sys;
use wkb::reader::Wkb;

use super::super::adapter;
use crate::error::GeoDataFusionResult;

// PostGIS returns a 2D GeometryCollection when the input has no triangulable
// area, even when the input carries a Z flag.
const EMPTY_GEOMETRY_COLLECTION_WKB: &[u8] = &[1, 7, 0, 0, 0, 0, 0, 0, 0];

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct TriangulatePolygon {
    signature: Signature,
}

impl TriangulatePolygon {
    pub fn new() -> Self {
        Self {
            signature: adapter::signature(),
        }
    }
}

impl Default for TriangulatePolygon {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for TriangulatePolygon {
    fn name(&self) -> &str {
        "st_triangulatepolygon"
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
        adapter::geometry_return_field(&args, &[0], self.name())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(triangulate_polygon_impl(&args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Computes the constrained Delaunay triangulation of polygonal input. GEOS returns a GeometryCollection of polygons, or an empty GeometryCollection when the input has no triangulable area. GEOS preserves Z. M ordinates are dropped before GEOS and are not present in the result.",
                "ST_TriangulatePolygon(geometry)",
            )
            .with_argument("geometry", "polygonal geometry")
            .build()
        }))
    }
}

fn triangulate_polygon_impl(args: &ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    adapter::unary_geos(args, |_row, _arrays, _original, geometry, writer| {
        let prepared = writer.write(geometry)?;
        let result = constrained_delaunay_triangulation(&prepared)?;
        let parsed = Wkb::try_new(&result).map_err(|error| {
            DataFusionError::Execution(format!(
                "ST_TriangulatePolygon result WKB is invalid: {error}"
            ))
        })?;
        if let geo_traits::GeometryType::GeometryCollection(collection) = parsed.as_type()
            && collection.num_geometries() == 0
        {
            return Ok(Some(EMPTY_GEOMETRY_COLLECTION_WKB.to_vec()));
        }
        Ok(Some(result))
    })
}

thread_local! {
    static CONSTRAINED_DELAUNAY_CONTEXT: ConstrainedDelaunayContext =
        ConstrainedDelaunayContext::new();
}

/// GEOS's high-level bindings do not expose
/// `GEOSConstrainedDelaunayTriangulation`, so this module owns a small
/// reentrant context for that one call. The public adapter continues to use
/// the bindings' shared context for every other operation.
struct ConstrainedDelaunayContext {
    handle: sys::GEOSContextHandle_t,
}

impl ConstrainedDelaunayContext {
    fn new() -> Self {
        Self {
            handle: unsafe { sys::GEOS_init_r() },
        }
    }
}

impl Drop for ConstrainedDelaunayContext {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe { sys::GEOS_finish_r(self.handle) };
        }
    }
}

fn constrained_delaunay_triangulation(wkb: &[u8]) -> GeoDataFusionResult<Vec<u8>> {
    CONSTRAINED_DELAUNAY_CONTEXT.with(|context| {
        let handle = context.handle;
        if handle.is_null() {
            return Err(DataFusionError::Execution(
                "ST_TriangulatePolygon could not initialize a GEOS context".to_string(),
            )
            .into());
        }

        unsafe {
            let input = sys::GEOSGeomFromWKB_buf_r(handle, wkb.as_ptr(), wkb.len());
            if input.is_null() {
                return Err(DataFusionError::Execution(
                    "ST_TriangulatePolygon could not read the input geometry".to_string(),
                )
                .into());
            }

            let output = sys::GEOSConstrainedDelaunayTriangulation_r(handle, input);
            sys::GEOSGeom_destroy_r(handle, input);
            if output.is_null() {
                return Err(DataFusionError::Execution(
                    "ST_TriangulatePolygon GEOS constrained triangulation failed".to_string(),
                )
                .into());
            }

            let wkb = write_wkb(handle, output);
            sys::GEOSGeom_destroy_r(handle, output);
            wkb
        }
    })
}

unsafe fn write_wkb(
    handle: sys::GEOSContextHandle_t,
    geometry: *const sys::GEOSGeometry,
) -> GeoDataFusionResult<Vec<u8>> {
    let writer = unsafe { sys::GEOSWKBWriter_create_r(handle) };
    if writer.is_null() {
        return Err(DataFusionError::Execution(
            "ST_TriangulatePolygon could not create a GEOS WKB writer".to_string(),
        )
        .into());
    }

    let dimension = unsafe { sys::GEOSGeom_getCoordinateDimension_r(handle, geometry) };
    let dimension = if dimension > 2 { 3 } else { 2 };
    unsafe { sys::GEOSWKBWriter_setOutputDimension_r(handle, writer, dimension) };

    let mut size = 0;
    let buffer = unsafe { sys::GEOSWKBWriter_write_r(handle, writer, geometry, &mut size) };
    unsafe { sys::GEOSWKBWriter_destroy_r(handle, writer) };
    if buffer.is_null() {
        return Err(DataFusionError::Execution(
            "ST_TriangulatePolygon could not encode the GEOS result".to_string(),
        )
        .into());
    }

    let bytes = unsafe { slice::from_raw_parts(buffer, size) }.to_vec();
    unsafe { sys::GEOSFree_r(handle, buffer.cast()) };
    Ok(bytes)
}

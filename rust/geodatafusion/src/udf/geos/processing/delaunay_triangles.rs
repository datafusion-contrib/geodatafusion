use std::sync::OnceLock;

use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geos::Geom;

use super::super::adapter;
use crate::error::GeoDataFusionResult;

// PostGIS returns typed 2D empty output when Delaunay has no triangles or
// edges, regardless of the input's Z flag.
const EMPTY_GEOMETRY_COLLECTION_WKB: &[u8] = &[1, 7, 0, 0, 0, 0, 0, 0, 0];
const EMPTY_MULTILINESTRING_WKB: &[u8] = &[1, 5, 0, 0, 0, 0, 0, 0, 0];

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct DelaunayTriangles {
    signature: Signature,
}

impl DelaunayTriangles {
    pub fn new() -> Self {
        Self {
            signature: adapter::signature(),
        }
    }
}

impl Default for DelaunayTriangles {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for DelaunayTriangles {
    fn name(&self) -> &str {
        "st_delaunaytriangles"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        adapter::check_arg_count(arg_types, &[1, 2, 3], self.name())?;
        adapter::check_geometry_type(&arg_types[0], 0, self.name())?;
        let mut coerced = arg_types.to_vec();
        if arg_types.len() > 1 {
            coerced[1] = adapter::coerce_float64(&arg_types[1], 1, self.name())?;
        }
        if arg_types.len() > 2 {
            coerced[2] = adapter::coerce_int64(&arg_types[2], 2, self.name())?;
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
        Ok(delaunay_triangles_impl(&args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Computes the Delaunay triangulation of the vertices of a geometry. The optional tolerance is passed to GEOS. Flags select the output: 0 returns polygons, 1 returns edges, and 2 requests TIN output, which GeoArrow cannot represent. GEOS preserves Z for polygon and edge output. M ordinates are dropped before GEOS and are not present in the result.",
                "ST_DelaunayTriangles(geometry, tolerance, flags)",
            )
            .with_argument("geometry", "geometry")
            .with_argument("tolerance", "optional float; defaults to 0")
            .with_argument("flags", "optional integer; 0 polygons, 1 edges, 2 TIN")
            .build()
        }))
    }
}

fn delaunay_triangles_impl(args: &ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    adapter::unary_geos(args, |row, arrays, _original, geometry, writer| {
        let tolerance = if arrays.len() > 1 {
            match adapter::float64_value(arrays, 1, row, "ST_DelaunayTriangles")? {
                Some(tolerance) => tolerance,
                None => return Ok(None),
            }
        } else {
            0.0
        };
        let flags = if arrays.len() > 2 {
            match adapter::int64_value(arrays, 2, row, "ST_DelaunayTriangles")? {
                Some(flags) => flags,
                None => return Ok(None),
            }
        } else {
            0
        };
        let only_edges = match flags {
            0 => false,
            1 => true,
            2 => {
                return Err(DataFusionError::Execution(
                    "ST_DelaunayTriangles flags=2 requests TIN output, which GeoArrow cannot represent"
                        .to_string(),
                )
                .into());
            }
            _ => {
                return Err(DataFusionError::Execution(format!(
                    "ST_DelaunayTriangles flags must be 0, 1, or 2, got {flags}"
                ))
                .into());
            }
        };

        let result = geometry.delaunay_triangulation(tolerance, only_edges)?;
        if result.is_empty()? {
            let empty = if only_edges {
                EMPTY_MULTILINESTRING_WKB
            } else {
                EMPTY_GEOMETRY_COLLECTION_WKB
            };
            return Ok(Some(empty.to_vec()));
        }
        Ok(Some(writer.write(&result)?))
    })
}

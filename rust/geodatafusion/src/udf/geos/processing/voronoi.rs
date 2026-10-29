use std::sync::OnceLock;

use arrow_array::Array;
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geoarrow_array::cast::to_wkb;
use geos::{Geom, Geometry};

use super::super::adapter;
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::geometry_array_from_arrow;

// Little-endian WKB for GEOMETRYCOLLECTION EMPTY.
const EMPTY_GEOMETRY_COLLECTION_WKB: &[u8] = &[1, 7, 0, 0, 0, 0, 0, 0, 0];

macro_rules! voronoi_udf {
    ($type:ident, $name:literal, $doc_name:ident, $description:literal, $example:literal, $only_edges:expr) => {
        #[derive(Debug, Eq, PartialEq, Hash)]
        pub struct $type {
            signature: Signature,
        }

        impl $type {
            pub fn new() -> Self {
                Self {
                    signature: adapter::signature(),
                }
            }
        }

        impl Default for $type {
            fn default() -> Self {
                Self::new()
            }
        }

        static $doc_name: OnceLock<Documentation> = OnceLock::new();

        impl ScalarUDFImpl for $type {
            fn name(&self) -> &str {
                $name
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
                    adapter::check_geometry_type(&arg_types[2], 2, self.name())?;
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
                Ok(voronoi_impl(&args, self.name(), $only_edges)?)
            }

            fn documentation(&self) -> Option<&Documentation> {
                Some($doc_name.get_or_init(|| {
                    Documentation::builder(DOC_SECTION_OTHER, $description, $example)
                        .with_argument("geometry", "geometry")
                        .with_argument("tolerance", "optional non-negative float; defaults to 0")
                        .with_argument(
                            "extend_to",
                            "optional geometry whose envelope extends the diagram",
                        )
                        .build()
                }))
            }
        }
    };
}

voronoi_udf!(
    VoronoiLines,
    "st_voronoilines",
    VORONOI_LINES_DOC,
    "Returns the boundaries of the Voronoi diagram of the input vertices as a MultiLineString. A null extend_to is treated as absent, matching the PostGIS default. Inputs with fewer than two vertices produce an empty GeometryCollection. GEOS computes the diagram in XY and the result never carries M.",
    "ST_VoronoiLines(geometry, tolerance, extend_to)",
    true
);
voronoi_udf!(
    VoronoiPolygons,
    "st_voronoipolygons",
    VORONOI_POLYGONS_DOC,
    "Returns the cells of the Voronoi diagram of the input vertices as a GeometryCollection. A null extend_to is treated as absent, matching the PostGIS default. Inputs with fewer than two vertices produce an empty GeometryCollection. GEOS computes the diagram in XY and the result never carries M.",
    "ST_VoronoiPolygons(geometry, tolerance, extend_to)",
    false
);

fn voronoi_impl(
    args: &ScalarFunctionArgs,
    name: &str,
    only_edges: bool,
) -> GeoDataFusionResult<ColumnarValue> {
    let has_extend_to = args.arg_fields.len() > 2;
    let nullable_arguments = if has_extend_to {
        &[2usize][..]
    } else {
        &[][..]
    };
    let extend_to = if has_extend_to && !matches!(args.arg_fields[2].data_type(), DataType::Null) {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let geometries =
            geometry_array_from_arrow(arrays[2].as_ref(), args.arg_fields[2].as_ref())?;
        Some(to_wkb::<i32>(geometries.as_ref())?)
    } else {
        None
    };

    adapter::unary_geos_with_nullable_arguments(
        args,
        nullable_arguments,
        |row, arrays, _original, geometry, writer| {
            let tolerance = if arrays.len() > 1 {
                match adapter::float64_value(arrays, 1, row, name)? {
                    Some(tolerance) => tolerance,
                    None => return Ok(None),
                }
            } else {
                0.0
            };
            if tolerance < 0.0 {
                return Err(DataFusionError::Execution(format!(
                    "{name} tolerance must be non-negative"
                ))
                .into());
            }

            if geometry.get_num_coordinates()? < 2 {
                return Ok(Some(EMPTY_GEOMETRY_COLLECTION_WKB.to_vec()));
            }

            let envelope = match &extend_to {
                None => None,
                Some(extend_to) if extend_to.inner().is_null(row) => None,
                Some(extend_to) => {
                    let bytes = extend_to.inner().value(row);
                    let prepared = adapter::prepare_geos_wkb(bytes)?;
                    Some(Geometry::new_from_wkb(prepared.as_ref())?)
                }
            };

            let result = geometry.voronoi(envelope.as_ref(), tolerance, only_edges)?;
            Ok(Some(writer.write(&result)?))
        },
    )
}

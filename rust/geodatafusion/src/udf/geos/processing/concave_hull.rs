use std::sync::{LazyLock, OnceLock};

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geoarrow_schema::CoordType;
use geos::{Geom, GeometryTypes};

use crate::error::GeoDataFusionResult;
use crate::udf::geos::adapter;

static SIGNATURE: LazyLock<Signature> = LazyLock::new(adapter::signature);

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct ConcaveHull {
    coord_type: CoordType,
}

impl ConcaveHull {
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for ConcaveHull {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

// Little-endian WKB for POLYGON EMPTY. PostGIS returns this for every empty
// concave-hull input, including empty XYM and XYZM inputs.
const EMPTY_POLYGON_WKB: &[u8] = &[1, 3, 0, 0, 0, 0, 0, 0, 0];

impl ScalarUDFImpl for ConcaveHull {
    fn name(&self) -> &str {
        "st_concavehull"
    }

    fn signature(&self) -> &Signature {
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        adapter::geometry_return_field_with_coord_type(
            &args,
            &[0],
            "ST_ConcaveHull",
            Some(self.coord_type),
        )
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(concave_hull_impl(args)?)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        adapter::check_arg_count(arg_types, &[2, 3], "ST_ConcaveHull")?;
        adapter::check_geometry_type(&arg_types[0], 0, "ST_ConcaveHull")?;

        let mut coerced = vec![
            arg_types[0].clone(),
            adapter::coerce_float64(&arg_types[1], 1, "ST_ConcaveHull")?,
        ];
        if let Some(data_type) = arg_types.get(2) {
            if matches!(data_type, DataType::Boolean | DataType::Null) {
                coerced.push(DataType::Boolean);
            } else {
                return Err(DataFusionError::Plan(format!(
                    "ST_ConcaveHull argument 3 must be boolean, got {data_type}"
                )));
            }
        }
        Ok(coerced)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Computes a possibly concave geometry containing all input vertices. The edge-length ratio must be between 0 and 1. Holes are omitted unless allowed explicitly.",
                "ST_ConcaveHull(geometry, ratio[, allow_holes])",
            )
            .with_argument("geometry", "geometry")
            .with_argument("ratio", "float in [0, 1]")
            .with_argument("allow_holes", "boolean")
            .build()
        }))
    }
}

fn concave_hull_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    adapter::unary_geos(&args, |row, arrays, _original, geometry, writer| {
        let ratio = adapter::float64_value(arrays, 1, row, "ST_ConcaveHull")?.ok_or_else(|| {
            DataFusionError::Internal(
                "ST_ConcaveHull ratio was null after adapter filtering".to_string(),
            )
        })?;
        if !(0.0..=1.0).contains(&ratio) {
            return Err(DataFusionError::Execution(format!(
                "ST_ConcaveHull edge-length ratio must be in [0, 1], got {ratio}"
            ))
            .into());
        }
        let holes_allowed = boolean_value(arrays, 2, row, "ST_ConcaveHull")?;
        if geometry.is_empty()? {
            return Ok(Some(EMPTY_POLYGON_WKB.to_vec()));
        }
        let output = match geometry.geometry_type()? {
            GeometryTypes::Polygon | GeometryTypes::MultiPolygon => {
                // Polygonal input uses GEOSConcaveHullOfPolygons, which keeps
                // the holes already present in the input and only lets the
                // hull create holes of its own when `allow_holes` is set.
                // Feed GEOS the normalized WKB so measure coordinates are
                // dropped consistently with the shared adapter.
                let prepared = writer.write(geometry)?;
                concave_hull_of_polygons(&prepared, ratio, holes_allowed)?
            }
            _ => writer.write(&geometry.concave_hull(ratio, holes_allowed)?)?,
        };
        Ok(Some(output))
    })
}

fn boolean_value(
    arrays: &[ArrayRef],
    position: usize,
    row: usize,
    name: &str,
) -> GeoDataFusionResult<bool> {
    let Some(array) = arrays.get(position) else {
        return Ok(false);
    };
    if !matches!(array.data_type(), DataType::Boolean) {
        return Err(DataFusionError::Internal(format!(
            "{name} argument {} was not coerced to Boolean",
            position + 1
        ))
        .into());
    }
    let values = array.as_boolean();
    Ok(!values.is_null(row) && values.value(row))
}

/// GEOS 3.11 does not expose `GEOSConcaveHullOfPolygons` through the Rust wrapper.
///
/// PostGIS uses this operation for polygonal inputs so the hull preserves the areas
/// covered by the input. The C API call here mirrors PostGIS's `isTight = false`
/// behavior and uses a fresh GEOS context for thread safety.
#[cfg(feature = "geos-3_11")]
fn concave_hull_of_polygons(
    wkb: &[u8],
    ratio: f64,
    allow_holes: bool,
) -> GeoDataFusionResult<Vec<u8>> {
    use geos::sys;

    struct Context(sys::GEOSContextHandle_t);

    impl Drop for Context {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe { sys::GEOS_finish_r(self.0) };
            }
        }
    }

    // All pointers below are created and released within this context. No GEOS
    // pointer escapes the function.
    unsafe {
        let context = Context(sys::GEOS_init_r());
        if context.0.is_null() {
            return Err(DataFusionError::Execution(
                "GEOS failed to initialize for ST_ConcaveHull".to_string(),
            )
            .into());
        }

        let reader = sys::GEOSWKBReader_create_r(context.0);
        if reader.is_null() {
            return Err(DataFusionError::Execution(
                "GEOS failed to create a WKB reader for ST_ConcaveHull".to_string(),
            )
            .into());
        }
        let input = sys::GEOSWKBReader_read_r(context.0, reader, wkb.as_ptr(), wkb.len());
        sys::GEOSWKBReader_destroy_r(context.0, reader);
        if input.is_null() {
            return Err(DataFusionError::Execution(
                "GEOS failed to parse polygonal ST_ConcaveHull input".to_string(),
            )
            .into());
        }

        let output =
            sys::GEOSConcaveHullOfPolygons_r(context.0, input, ratio, 0, allow_holes.into());
        sys::GEOSGeom_destroy_r(context.0, input);
        if output.is_null() {
            return Err(DataFusionError::Execution(
                "GEOS ST_ConcaveHullOfPolygons failed".to_string(),
            )
            .into());
        }

        let writer = sys::GEOSWKBWriter_create_r(context.0);
        if writer.is_null() {
            sys::GEOSGeom_destroy_r(context.0, output);
            return Err(DataFusionError::Execution(
                "GEOS failed to create a WKB writer for ST_ConcaveHull".to_string(),
            )
            .into());
        }
        sys::GEOSWKBWriter_setOutputDimension_r(context.0, writer, 3);
        let mut size = 0;
        let buffer = sys::GEOSWKBWriter_write_r(context.0, writer, output, &mut size);
        sys::GEOSWKBWriter_destroy_r(context.0, writer);
        sys::GEOSGeom_destroy_r(context.0, output);
        if buffer.is_null() {
            return Err(DataFusionError::Execution(
                "GEOS failed to serialize ST_ConcaveHull output".to_string(),
            )
            .into());
        }

        let bytes = std::slice::from_raw_parts(buffer, size).to_vec();
        sys::GEOSFree_r(context.0, buffer.cast());
        Ok(bytes)
    }
}

#[cfg(test)]
mod test {
    use arrow_array::cast::AsArray;
    use arrow_array::types::Float64Type;
    use datafusion::prelude::SessionContext;
    use geo_traits::GeometryTrait;
    use geoarrow_array::GeoArrowArrayAccessor;
    use geoarrow_schema::GeometryType;

    use super::*;
    use crate::udf::native::io::GeomFromText;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        crate::register(&ctx);
        ctx
    }

    async fn text_result(ctx: &SessionContext, sql: &str) -> String {
        ctx.sql(sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
            .column(0)
            .as_string::<i32>()
            .value(0)
            .to_string()
    }

    async fn float_result(ctx: &SessionContext, sql: &str) -> f64 {
        ctx.sql(sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
            .column(0)
            .as_primitive::<Float64Type>()
            .value(0)
    }

    #[tokio::test]
    async fn concave_hull_matches_postgis_point_line_and_empty_cases() {
        let ctx = ctx();
        let cases = [
            ("MULTIPOINT((1 2))", "0.5", "false", "POINT(1 2)"),
            (
                "MULTIPOINT((0 0),(1 1),(2 2))",
                "0.5",
                "false",
                "LINESTRING(0 0,2 2)",
            ),
            ("POINT EMPTY", "0.5", "false", "POLYGON EMPTY"),
            (
                "GEOMETRYCOLLECTION(POINT(1 2))",
                "0.5",
                "false",
                "POINT(1 2)",
            ),
            (
                "GEOMETRYCOLLECTION(POINT EMPTY)",
                "0.5",
                "false",
                "POLYGON EMPTY",
            ),
            (
                "MULTIPOINT Z ((0 0 1),(1 0 2),(0 1 3),(1 1 4))",
                "0.5",
                "false",
                "POLYGON Z((1 0 2,0 0 1,0 1 3,1 1 4,1 0 2))",
            ),
            ("POINT M (1 2 3)", "0.5", "false", "POINT(1 2)"),
            ("POINT ZM (1 2 3 4)", "0.5", "false", "POINT Z(1 2 3)"),
            ("POINT M EMPTY", "0.5", "false", "POLYGON EMPTY"),
            ("LINESTRING M EMPTY", "0.5", "false", "POLYGON EMPTY"),
            ("POLYGON ZM EMPTY", "0.5", "false", "POLYGON EMPTY"),
        ];

        for (geometry, ratio, allow_holes, expected) in cases {
            let actual = text_result(
                &ctx,
                &format!(
                    "SELECT ST_AsText(ST_ConcaveHull(ST_GeomFromText('{geometry}'), {ratio}, {allow_holes}))"
                ),
            )
            .await;
            assert_eq!(actual, expected, "geometry: {geometry}");
        }
    }

    #[tokio::test]
    async fn concave_hull_uses_polygon_aware_contract() {
        let ctx = ctx();
        for empty in ["POLYGON EMPTY", "MULTIPOLYGON EMPTY"] {
            let actual = text_result(
                &ctx,
                &format!("SELECT ST_AsText(ST_ConcaveHull(ST_GeomFromText('{empty}'), 0.5))"),
            )
            .await;
            assert_eq!(actual, "POLYGON EMPTY");
        }

        // The hole belongs to the polygonal input, so PostGIS keeps it for
        // both flag values: allow_holes only controls holes the hull creates
        // between separate input polygons.
        let without_holes = text_result(
            &ctx,
            "SELECT ST_AsText(ST_ConcaveHull(ST_GeomFromText('POLYGON((0 0,0 10,10 10,10 0,0 0),(4 4,4 6,6 6,6 4,4 4))'), 0.5, false))",
        )
        .await;
        assert_eq!(
            without_holes,
            "POLYGON((0 0,0 10,10 10,10 0,0 0),(4 4,4 6,6 6,6 4,4 4))"
        );

        let with_holes = text_result(
            &ctx,
            "SELECT ST_AsText(ST_ConcaveHull(ST_GeomFromText('POLYGON((0 0,0 10,10 10,10 0,0 0),(4 4,4 6,6 6,6 4,4 4))'), 0.5, true))",
        )
        .await;
        assert_eq!(
            with_holes,
            "POLYGON((0 0,0 10,10 10,10 0,0 0),(4 4,4 6,6 6,6 4,4 4))"
        );

        // GEOS 3.11 returns the same ring topology with a different starting
        // vertex than GEOS 3.15, so compare topology, type, dimension, and
        // validity instead of serialized vertex order.
        let expected = "POLYGON((1 1,3 1,4 1,4 0,3 0,1 0,0 0,0 1,1 1))";
        let batch = ctx
            .sql(&format!(
                "SELECT hull,
                        ST_Equals(hull, ST_GeomFromText('{expected}')) AS equal,
                        arrow_cast(ST_GeometryType(hull), 'Utf8') AS kind,
                        ST_IsValid(hull) AS valid
                 FROM (
                    SELECT ST_ConcaveHull(
                        ST_GeomFromText('MULTIPOLYGON(((0 0,0 1,1 1,1 0,0 0)),((3 0,3 1,4 1,4 0,3 0)))'),
                        0.5
                    ) AS hull
                 ) AS s"
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert!(batch.column(1).as_boolean().value(0));
        assert_eq!(batch.column(2).as_string::<i32>().value(0), "ST_Polygon");
        assert!(batch.column(3).as_boolean().value(0));
        let geometry = geoarrow_array::array::GeometryArray::try_from((
            batch.column(0).as_ref(),
            batch.schema_ref().field(0),
        ))
        .unwrap();
        assert_eq!(geometry.value(0).unwrap().dim(), geo_traits::Dimensions::Xy);

        let polygon_z = text_result(
            &ctx,
            "SELECT ST_AsText(ST_ConcaveHull(ST_GeomFromText('POLYGON Z ((0 0 1,0 10 2,10 10 3,10 0 4,0 0 1),(4 4 5,4 6 6,6 6 7,6 4 8,4 4 5))'), 0.5, true))",
        )
        .await;
        assert_eq!(
            polygon_z,
            "POLYGON Z((0 0 1,0 10 2,10 10 3,10 0 4,0 0 1),(4 4 5,4 6 6,6 6 7,6 4 8,4 4 5))"
        );
    }

    #[tokio::test]
    async fn concave_hull_handles_nulls_and_rejects_invalid_ratio() {
        let ctx = ctx();
        let batch = ctx
            .sql(
                "SELECT \
                    ST_ConcaveHull(ST_GeomFromText(CAST(NULL AS TEXT)), 0.5) IS NULL AS null_geometry, \
                    ST_ConcaveHull(ST_GeomFromText('POINT(1 2)'), CAST(NULL AS DOUBLE)) IS NULL AS null_ratio, \
                    ST_ConcaveHull(ST_GeomFromText('POINT(1 2)'), 0.5, CAST(NULL AS BOOLEAN)) IS NULL AS null_holes",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        for column in 0..3 {
            assert!(batch.column(column).as_boolean().value(0));
        }

        for ratio in [
            "-0.1",
            "1.1",
            "CAST('NaN' AS DOUBLE)",
            "CAST('Infinity' AS DOUBLE)",
        ] {
            let error = ctx
                .sql(&format!(
                    "SELECT ST_ConcaveHull(ST_GeomFromText('POINT(1 2)'), {ratio})"
                ))
                .await
                .unwrap()
                .collect()
                .await
                .expect_err("invalid ratio must fail");
            assert!(
                error.to_string().contains("ratio"),
                "unexpected error: {error}"
            );
        }

        // PostGIS drops M before calling GEOS while preserving Z. Polygon
        // output vertex order is GEOS-version dependent, so assert the
        // observable contract instead of a fixed ring order.
        let measure_only = text_result(
            &ctx,
            "SELECT ST_AsText(ST_ConcaveHull(\
                ST_GeomFromText('MULTIPOINT M ((0 0 1),(1 0 2),(0 1 3),(1 1 4))'), \
                0.5\
            ))",
        )
        .await;
        assert!(measure_only.starts_with("POLYGON("), "{measure_only}");
        let measure_only_area = float_result(
            &ctx,
            "SELECT ST_Area(ST_ConcaveHull(\
                ST_GeomFromText('MULTIPOINT M ((0 0 1),(1 0 2),(0 1 3),(1 1 4))'), \
                0.5\
            ))",
        )
        .await;
        assert!((measure_only_area - 1.0).abs() < 1e-12);

        let measure_and_z = text_result(
            &ctx,
            "SELECT ST_AsText(ST_ConcaveHull(\
                ST_GeomFromText('MULTIPOINT ZM ((0 0 1 5),(1 0 2 6),(0 1 3 7),(1 1 4 8))'), \
                0.5\
            ))",
        )
        .await;
        assert!(measure_and_z.starts_with("POLYGON Z("), "{measure_and_z}");
        let measure_and_z_area = float_result(
            &ctx,
            "SELECT ST_Area(ST_ConcaveHull(\
                ST_GeomFromText('MULTIPOINT ZM ((0 0 1 5),(1 0 2 6),(0 1 3 7),(1 1 4 8))'), \
                0.5\
            ))",
        )
        .await;
        assert!((measure_and_z_area - 1.0).abs() < 1e-12);
    }

    #[tokio::test]
    async fn concave_hull_honors_constructor_coord_type() {
        let ctx = SessionContext::new();
        ctx.register_udf(GeomFromText::default().into());
        ctx.register_udf(ConcaveHull::new(CoordType::Separated).into());

        let batch = ctx
            .sql(
                "SELECT ST_ConcaveHull(\
                    ST_GeomFromText('MULTIPOINT((0 0),(1 0),(0 1),(1 1))'), \
                    0.5\
                )",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let geometry_type = batch
            .schema()
            .field(0)
            .try_extension_type::<GeometryType>()
            .unwrap();
        assert_eq!(geometry_type.coord_type(), CoordType::Separated);
    }
}

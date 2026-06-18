use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow_array::BooleanArray;
use arrow_array::builder::BooleanBuilder;
use arrow_schema::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo_traits::{
    GeometryCollectionTrait, GeometryTrait, LineStringTrait, MultiLineStringTrait, MultiPointTrait,
    MultiPolygonTrait, PointTrait, PolygonTrait,
};
use geoarrow_array::{GeoArrowArrayAccessor, WrapArray, downcast_geoarrow_array};
use geoarrow_schema::GeoArrowType;

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct IsEmpty;

impl IsEmpty {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for IsEmpty {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for IsEmpty {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_isempty"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(is_empty_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Tests if a geometry is topologically empty. \
                 Multi-geometries and GeometryCollections where every leaf is empty \
                 (e.g. GEOMETRYCOLLECTION(POINT EMPTY, POLYGON EMPTY) are reported empty. \
                 ST_IsEmpty(NULL) is NULL.",
                "ST_IsEmpty(geom)",
            )
            .with_argument("geom", "geometry")
            .build()
        }))
    }
}

/// Recursive emptiness predicate.
///
/// This matches the behavior of PostGIS and JTS.
/// As a cross-reference, the javadoc for JTS `Geometry.isEmpty()` reads as follows:
/// "the emptiness test checks for topological emptiness,
/// not structural emptiness. A collection containing only empty elements is reported as empty."
pub(crate) fn is_geometry_topologically_empty(geom: &impl GeometryTrait<T = f64>) -> bool {
    match geom.as_type() {
        geo_traits::GeometryType::Point(p) => p.coord().is_none(),
        geo_traits::GeometryType::LineString(ls) => ls.num_coords() == 0,
        geo_traits::GeometryType::Polygon(p) => {
            p.exterior().is_none_or(|ring| ring.num_coords() == 0)
        }
        geo_traits::GeometryType::MultiPoint(mp) => mp.points().all(|p| p.coord().is_none()),
        geo_traits::GeometryType::MultiLineString(mls) => {
            mls.line_strings().all(|ls| ls.num_coords() == 0)
        }
        geo_traits::GeometryType::MultiPolygon(mp) => mp
            .polygons()
            .all(|p| p.exterior().is_none_or(|ring| ring.num_coords() == 0)),
        geo_traits::GeometryType::GeometryCollection(gc) => gc
            .geometries()
            .all(|child| is_geometry_topologically_empty(&child)),
        // Rect/Triangle/Line always carry coordinates and so are never empty.
        // NB: geoarrow has no curved-geometry support, so e.g. CIRCULARSTRING EMPTY
        // from the PostGIS docs is not representable at the moment, but we can still model them.
        geo_traits::GeometryType::Rect(_)
        | geo_traits::GeometryType::Triangle(_)
        | geo_traits::GeometryType::Line(_) => false,
    }
}

fn is_empty_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(&args.args)?
        .into_iter()
        .next()
        .unwrap();
    let geo_type = GeoArrowType::from_arrow_field(&args.arg_fields[0])?;
    let geo_array = geo_type.wrap_array(&array)?;
    let geo_array_ref = geo_array.as_ref();

    let result = downcast_geoarrow_array!(geo_array_ref, impl_is_empty)?;

    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn impl_is_empty<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoDataFusionResult<BooleanArray> {
    let mut builder = BooleanBuilder::with_capacity(array.len());

    for item in array.iter() {
        match item {
            // A present geometry.
            // Emptiness is recursive so that it's topological rather than just the structure.
            // so a collection where every leaf is empty counts as empty itself.
            Some(geom) => {
                let geom = geom?;
                builder.append_value(is_geometry_topologically_empty(&geom));
            }
            // SQL NULL in, SQL NULL out.
            // This matches the PostGIS behavior,
            // which explicitly calls out its behavior as non-conforming to SQL-MM.
            None => {
                builder.append_null();
            }
        }
    }

    Ok(builder.finish())
}

#[cfg(test)]
mod test {
    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::udf::native::io::GeomFromText;

    #[tokio::test]
    async fn test_st_isempty() {
        let ctx = SessionContext::new();
        ctx.register_udf(IsEmpty::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        // Explicitly noted examples come from the PostGIS documentation (CC-BY-SA-3.0).
        let cases = vec![
            (
                "GEOMETRYCOLLECTION EMPTY",
                true,
                "PostGIS doc example: empty geometry collection",
            ),
            ("POLYGON EMPTY", true, "PostGIS doc example: empty polygon"),
            // Degenerate (collinear, zero-area) but NOT empty: emptiness is about the
            // presence of coordinates, not area.
            (
                "POLYGON((1 2, 3 4, 5 6, 1 2))",
                false,
                "PostGIS doc example: degenerate but non-empty polygon",
            ),
            // Empty geometries of every supported type are empty.
            ("POINT EMPTY", true, "empty point"),
            ("LINESTRING EMPTY", true, "empty linestring"),
            ("MULTIPOINT EMPTY", true, "empty multipoint"),
            ("MULTILINESTRING EMPTY", true, "empty multilinestring"),
            ("MULTIPOLYGON EMPTY", true, "empty multipolygon"),
            // Negative examples: a populated geometry of each type is never empty.
            ("POINT(1 2)", false, "non-empty point"),
            ("LINESTRING(0 0, 1 1)", false, "non-empty linestring"),
            (
                "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
                false,
                "non-empty polygon",
            ),
            ("MULTIPOINT(0 0, 1 1)", false, "non-empty multipoint"),
            (
                "MULTILINESTRING((0 0, 1 1))",
                false,
                "non-empty multilinestring",
            ),
            (
                "MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)))",
                false,
                "non-empty multipolygon",
            ),
            (
                "GEOMETRYCOLLECTION(POINT(0 0))",
                false,
                "non-empty geometry collection",
            ),
            // Recursive emptiness (PostGIS / JTS topological semantics).
            // A collection whose every leaf is empty is itself reported as empty,
            // even though it has structurally non-zero children.
            //
            // NB: GeoArrow explicitly denies recursive collections,
            // so we only need to test one level.
            (
                "GEOMETRYCOLLECTION(POINT EMPTY, POLYGON EMPTY)",
                true,
                "collection of all-empty atomic children is recursively empty",
            ),
            (
                "GEOMETRYCOLLECTION(MULTIPOINT EMPTY, MULTIPOLYGON EMPTY)",
                true,
                "collection of all-empty multi-children is recursively empty",
            ),
            (
                "GEOMETRYCOLLECTION(POINT(0 0), POLYGON EMPTY)",
                false,
                "collection with at least one non-empty child is non-empty",
            ),
        ];

        for (wkt, expected, description) in cases {
            let sql = format!("SELECT ST_IsEmpty(ST_GeomFromText('{}'))", wkt);
            let df = ctx
                .sql(&sql)
                .await
                .unwrap_or_else(|_| panic!("Failed to execute SQL for {}", description));

            let batch = df.collect().await.unwrap().into_iter().next().unwrap();
            let col = batch.column(0).as_boolean();

            assert_eq!(col.value(0), expected, "Failed on {}: {}", description, wkt);
        }
    }

    #[tokio::test]
    async fn test_st_isempty_null() {
        let ctx = SessionContext::new();
        ctx.register_udf(IsEmpty::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        // PostGIS explicitly calls out ST_IsEmpty(NULL) as NULL in the docs.
        let df = ctx
            .sql("SELECT ST_IsEmpty(ST_GeomFromText(NULL))")
            .await
            .unwrap();

        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0).as_boolean();

        assert!(col.is_null(0), "ST_IsEmpty(NULL) should be NULL");
    }
}

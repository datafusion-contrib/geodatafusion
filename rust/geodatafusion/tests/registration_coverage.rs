use std::collections::BTreeSet;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use geodatafusion::CoordType;

const IMPLEMENTED: &str = "\u{2705}";

#[tokio::test]
async fn group_registration_applies_coordinate_layout() {
    use geoarrow_array::array::from_arrow_array;
    use geoarrow_schema::GeoArrowType;

    let groups: [(fn(&SessionContext, CoordType), &str); 4] = [
        (
            geodatafusion::register_with_coord_type,
            "SELECT ST_Point(1, 2), ST_Centroid('POINT(1 2)'), ST_PointFromGeoHash('s')",
        ),
        (
            geodatafusion::udf::native::register_with_coord_type,
            "SELECT ST_Point(1, 2), ST_EndPoint('LINESTRING(0 0,1 2)'), \
             ST_Expand('POINT(1 2)', 1), ST_Force2D('POINT Z(1 2 3)'), ST_GeomFromText('POINT(1 2)')",
        ),
        (
            geodatafusion::udf::geo::register_with_coord_type,
            "SELECT ST_Centroid('POINT(1 2)'), ST_Simplify('LINESTRING(0 0,1 1)', 0.1)",
        ),
        (
            geodatafusion::udf::geohash::register_with_coord_type,
            "SELECT ST_PointFromGeoHash('s')",
        ),
    ];
    for layout in [CoordType::Separated, CoordType::Interleaved] {
        for (register, sql) in groups {
            let ctx = SessionContext::new_with_state(SessionStateBuilder::new().build());
            register(&ctx, layout);
            let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
            let batch = &batches[0];
            for (field, array) in batch.schema().fields().iter().zip(batch.columns()) {
                assert_eq!(
                    GeoArrowType::from_arrow_field(field).unwrap().coord_type(),
                    Some(layout)
                );
                let geometry = from_arrow_array(array.as_ref(), field).unwrap();
                assert_eq!(geometry.data_type().coord_type(), Some(layout));
                assert_eq!(array.len(), 1);
                assert!(!geometry.is_null(0));
            }
        }
    }
}

fn postgis_compatibility_implemented_functions() -> BTreeSet<String> {
    include_str!("../../../POSTGIS_COMPATIBILITY.md")
        .lines()
        .filter_map(|line| {
            let row = line.strip_prefix('|')?;
            let mut cells = row.split('|').map(str::trim);
            let name = cells.next()?;
            let implemented = cells.next()?;
            (implemented == IMPLEMENTED).then(|| name.to_ascii_lowercase())
        })
        .collect()
}

fn readme_supported_functions() -> BTreeSet<String> {
    let readme = include_str!("../../../README.md");
    let supported_section = readme
        .split_once("## Supported Functions")
        .map(|(_, rest)| rest)
        .expect("README.md must contain a `## Supported Functions` section");

    supported_section
        .lines()
        .filter_map(|line| {
            let row = line.strip_prefix('|')?;
            let mut cells = row.split('|').map(str::trim);
            let name = cells.next()?.trim_matches('`');
            if name.is_empty() || name == "Name" || name.starts_with('-') {
                return None;
            }
            Some(name.to_ascii_lowercase())
        })
        .collect()
}

fn registered_functions() -> BTreeSet<String> {
    // Start without DataFusion built-ins so only geodatafusion registrations are inspected.
    let ctx = SessionContext::new_with_state(SessionStateBuilder::new().build());
    geodatafusion::register(&ctx);

    let state = ctx.state();
    state
        .scalar_functions()
        .keys()
        .chain(state.aggregate_functions().keys())
        .cloned()
        .collect()
}

#[test]
fn readme_implemented_functions_match_public_registration() {
    for (doc_name, expected) in [
        (
            "POSTGIS_COMPATIBILITY.md",
            postgis_compatibility_implemented_functions(),
        ),
        ("README.md", readme_supported_functions()),
    ] {
        let actual = registered_functions();
        let missing_from_registry = expected.difference(&actual).collect::<Vec<_>>();
        let missing_from_doc = actual.difference(&expected).collect::<Vec<_>>();

        assert!(
            missing_from_registry.is_empty() && missing_from_doc.is_empty(),
            "{doc_name} and geodatafusion::register() disagree\n\
             {doc_name} lists implemented but register() does not expose: {missing_from_registry:?}\n\
             register() exposes but {doc_name} does not list: {missing_from_doc:?}"
        );
    }
}

#[test]
fn registered_functions_have_documentation() {
    let ctx = SessionContext::new_with_state(SessionStateBuilder::new().build());
    geodatafusion::register(&ctx);

    let state = ctx.state();
    for (name, udf) in state.scalar_functions() {
        let doc = udf
            .documentation()
            .unwrap_or_else(|| panic!("scalar UDF `{name}` is missing documentation"));
        assert!(
            !doc.description.trim().is_empty(),
            "scalar UDF `{name}` has an empty documentation description"
        );
        assert!(
            !doc.syntax_example.trim().is_empty(),
            "scalar UDF `{name}` has an empty syntax_example"
        );
        if let Some(related) = &doc.related_udfs {
            for rel in related {
                assert_eq!(
                    rel,
                    &rel.to_ascii_lowercase(),
                    "scalar UDF `{name}` has non-lowercase related_udf `{rel}`"
                );
                assert_ne!(
                    rel,
                    udf.name(),
                    "scalar UDF `{name}` lists itself in related_udfs"
                );
            }
        }
    }

    for (name, udaf) in state.aggregate_functions() {
        let doc = udaf
            .documentation()
            .unwrap_or_else(|| panic!("aggregate UDF `{name}` is missing documentation"));
        assert!(
            !doc.description.trim().is_empty(),
            "aggregate UDF `{name}` has an empty documentation description"
        );
        assert!(
            !doc.syntax_example.trim().is_empty(),
            "aggregate UDF `{name}` has an empty syntax_example"
        );
    }
}

use std::collections::BTreeSet;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;

const IMPLEMENTED: &str = "\u{2705}";
const GEOS_GATED_FUNCTIONS: &[&str] = &["st_linemerge"];

fn readme_implemented_functions() -> BTreeSet<String> {
    include_str!("../../../README.md")
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
    let expected = readme_implemented_functions();

    for name in GEOS_GATED_FUNCTIONS {
        assert!(expected.contains(*name));
    }
    #[cfg(not(feature = "geos-3_11"))]
    let expected = expected
        .into_iter()
        .filter(|name| !GEOS_GATED_FUNCTIONS.contains(&name.as_str()))
        .collect::<BTreeSet<_>>();

    let actual = registered_functions();
    let missing_from_registry = expected.difference(&actual).collect::<Vec<_>>();
    let missing_from_readme = actual.difference(&expected).collect::<Vec<_>>();

    assert!(
        missing_from_registry.is_empty() && missing_from_readme.is_empty(),
        "README and geodatafusion::register() disagree\n\
         README marks implemented but register() does not expose: {missing_from_registry:?}\n\
         register() exposes but README does not mark implemented: {missing_from_readme:?}"
    );
}

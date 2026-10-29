//! Feature-gated PostGIS oracle coverage for the advanced GEOS processing
//! UDFs and for the processing functions corrected for GEOS version
//! differences. The shared discovery list is maintained separately; these
//! groups keep their own `geos-3_11` gate so default-feature runs stay green
//! and so unrelated group failures cannot hide their result on the minimum
//! supported GEOS version.

#[cfg(feature = "geos-3_11")]
#[path = "support/oracle.rs"]
#[allow(dead_code)]
mod oracle;

#[cfg(feature = "geos-3_11")]
#[tokio::test]
async fn advanced_geos_groups_match_reviewed_baselines()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use datafusion::prelude::SessionContext;

    const GROUPS: &[&str] = &[
        "geos/is_valid_detail",
        "geos/make_valid",
        "geos/make_valid_default",
        "geos/clip_by_box2d",
        "geos/split",
        "geos/subdivide",
        "geos/subdivide_default",
        "geos/simplify_polygon_hull",
        "geos/simplify_polygon_hull_default",
        "geos/line_merge",
        "geos/reduce_precision",
    ];

    let context = SessionContext::new();
    geodatafusion::register(&context);

    let mut total = oracle::Stats::default();
    for name in GROUPS {
        let group = oracle::load_group(name, true)?;
        let stats = oracle::run_offline(&context, &group).await?;
        eprintln!(
            "postgis oracle offline group={} {}",
            group.name,
            stats.summary()
        );
        total.add(&stats);
    }
    total.ensure_compared("advanced GEOS oracle offline")?;
    eprintln!("postgis oracle advanced total {}", total.summary());
    Ok(())
}

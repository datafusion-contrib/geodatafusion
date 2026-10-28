//! PostGIS differential tests.
//!
//! Ordinary runs read reviewed, typed baselines and never need Docker. The
//! ignored live test requires `GEODATAFUSION_POSTGIS_URL` and checks both the
//! saved baseline and the current DataFusion results against the live oracle.

#[path = "support/oracle.rs"]
mod oracle;

use std::path::PathBuf;

use datafusion::prelude::SessionContext;
use oracle::Stats;

type TestResult = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test]
async fn reviewed_baselines_match_datafusion() -> TestResult {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);

    let mut total = Stats::default();
    for &group_name in oracle::GROUPS {
        let group = oracle::load_group(group_name, true)?;
        let stats = oracle::run_offline(&ctx, &group).await?;
        eprintln!(
            "postgis oracle offline group={} {}",
            group.name,
            stats.summary()
        );
        total.add(&stats);
    }
    total.ensure_compared("offline DataFusion vs reviewed baselines")?;
    eprintln!("postgis oracle offline total {}", total.summary());

    Ok(())
}

#[tokio::test]
#[ignore = "requires GEODATAFUSION_POSTGIS_URL pointing at the pinned PostGIS reference"]
async fn live_postgis_matches_baseline_and_datafusion() -> TestResult {
    let dsn = std::env::var("GEODATAFUSION_POSTGIS_URL").map_err(
        |_| "GEODATAFUSION_POSTGIS_URL must be set when running the ignored live PostGIS test",
    )?;
    let image = std::env::var("GEODATAFUSION_POSTGIS_IMAGE")
        .unwrap_or_else(|_| oracle::PINNED_POSTGIS_IMAGE.to_string());
    let write_dir = std::env::var_os("GEODATAFUSION_ORACLE_WRITE").map(PathBuf::from);

    let client = oracle::connect(&dsn).await?;
    let versions = oracle::read_oracle_versions(&client, &image).await?;
    eprintln!(
        "postgis oracle live reference image={} postgresql={} postgis={} geos={}",
        versions.image,
        versions.postgresql_version,
        versions.postgis_version,
        versions.geos_version
    );

    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);

    let mut baseline_total = Stats::default();
    let mut datafusion_total = Stats::default();
    for &group_name in oracle::GROUPS {
        let group = if write_dir.is_some() {
            oracle::load_capture_group(group_name)?
        } else {
            oracle::load_group(group_name, true)?
        };
        let (baseline_stats, datafusion_stats) =
            oracle::run_live(&ctx, &group, &client, &versions, write_dir.as_deref()).await?;
        eprintln!(
            "postgis oracle live group={} baseline {}",
            group.name,
            baseline_stats.summary()
        );
        eprintln!(
            "postgis oracle live group={} datafusion {}",
            group.name,
            datafusion_stats.summary()
        );
        baseline_total.add(&baseline_stats);
        datafusion_total.add(&datafusion_stats);
    }

    if write_dir.is_none() {
        baseline_total.ensure_compared("live PostGIS vs reviewed baselines")?;
    }
    datafusion_total.ensure_compared("live DataFusion vs PostGIS")?;
    eprintln!(
        "postgis oracle live baseline total {}",
        baseline_total.summary()
    );
    eprintln!(
        "postgis oracle live datafusion total {}",
        datafusion_total.summary()
    );

    Ok(())
}

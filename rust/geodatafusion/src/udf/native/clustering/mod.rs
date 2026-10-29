//! PostGIS clustering functions implemented natively with planar Rust algorithms.
//!
//! The window functions (`ST_ClusterDBSCAN`, `ST_ClusterIntersectingWin`,
//! `ST_ClusterKMeans`, `ST_ClusterWithinWin`) are real DataFusion window UDFs and process the
//! whole `PARTITION BY` group. The aggregates (`ST_ClusterIntersecting`, `ST_ClusterWithin`)
//! return a Dump-like `List<Struct{geom: geoarrow.wkb}>`, one `geom` per cluster, and preserve
//! their state across partial aggregation.

mod aggregate;
mod algorithms;
mod common;
mod kmeans;
mod window;

pub use aggregate::{ClusterIntersecting, ClusterWithin};
pub use window::ClusteringWindow;

/// Register all clustering functions.
pub fn register(session_context: &datafusion::prelude::SessionContext) {
    session_context.register_udaf(ClusterIntersecting::new().into());
    session_context.register_udaf(ClusterWithin::new().into());
    session_context.register_udwf(ClusteringWindow::dbscan().into());
    session_context.register_udwf(ClusteringWindow::cluster_intersecting_win().into());
    session_context.register_udwf(ClusteringWindow::cluster_within_win().into());
    session_context.register_udwf(ClusteringWindow::cluster_kmeans().into());
}

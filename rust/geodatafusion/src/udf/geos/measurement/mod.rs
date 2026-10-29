//! Planar measurement UDFs backed by GEOS.

mod closest_point;
mod frechet_distance;
mod furthest;
mod hausdorff_distance;
mod longest_line;
mod max_distance;
mod minimum_clearance;
mod shortest_line;

pub use closest_point::ClosestPoint;
pub use frechet_distance::FrechetDistance;
pub use hausdorff_distance::HausdorffDistance;
pub use longest_line::LongestLine;
pub use max_distance::MaxDistance;
pub use minimum_clearance::{MinimumClearance, MinimumClearanceLine};
pub use shortest_line::ShortestLine;

pub fn register(session_context: &datafusion::prelude::SessionContext) {
    session_context.register_udf(ClosestPoint.into());
    session_context.register_udf(FrechetDistance.into());
    session_context.register_udf(HausdorffDistance.into());
    session_context.register_udf(LongestLine.into());
    session_context.register_udf(MaxDistance.into());
    session_context.register_udf(MinimumClearance.into());
    session_context.register_udf(MinimumClearanceLine.into());
    session_context.register_udf(ShortestLine.into());
}

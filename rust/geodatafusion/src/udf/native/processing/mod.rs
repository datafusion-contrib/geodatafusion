//! Native geometry processing algorithms.

mod chaikin_smoothing;
mod filter_by_m;
mod generate_points;
mod geometric_median;
mod hershey_data;
mod letters;
mod set_effective_area;
mod util;

use datafusion::prelude::SessionContext;

/// Register all native geometry-processing UDFs.
pub fn register(context: &SessionContext) {
    context.register_udf(chaikin_smoothing::ChaikinSmoothing::default().into());
    context.register_udf(filter_by_m::FilterByM::default().into());
    context.register_udf(generate_points::GeneratePoints::default().into());
    context.register_udf(geometric_median::GeometricMedian::default().into());
    context.register_udf(letters::Letters::default().into());
    context.register_udf(set_effective_area::SetEffectiveArea::default().into());
}

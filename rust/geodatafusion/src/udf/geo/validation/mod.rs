mod is_valid;
mod is_valid_reason;

use geo::Validation as _;
use geo_traits::GeometryTrait;
use geoarrow_schema::error::GeoArrowResult;

pub use is_valid::IsValid;
pub use is_valid_reason::IsValidReason;

use crate::udf::geo::util::with_owned_geo_geometry;

pub(super) fn validation_error(
    geometry: &impl GeometryTrait<T = f64>,
) -> GeoArrowResult<Option<String>> {
    Ok(with_owned_geo_geometry!(geometry, |owned| {
        owned
            .check_validation()
            .err()
            .map(|error| error.to_string())
    }))
}

pub fn register(session_context: &datafusion::prelude::SessionContext) {
    session_context.register_udf(IsValid.into());
    session_context.register_udf(IsValidReason.into());
}

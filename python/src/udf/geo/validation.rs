use geodatafusion::udf::geo::validation::{IsValid, IsValidReason};

use crate::impl_udf;

impl_udf!(IsValid, PyIsValid, "IsValid", "geodatafusion.geo");
impl_udf!(
    IsValidReason,
    PyIsValidReason,
    "IsValidReason",
    "geodatafusion.geo"
);

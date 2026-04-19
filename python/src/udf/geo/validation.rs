use geodatafusion::udf::geo::validation::{IsClosed, IsValid, IsValidReason};

use crate::impl_udf;

impl_udf!(IsClosed, PyIsClosed, "IsClosed");
impl_udf!(IsValid, PyIsValid, "IsValid");
impl_udf!(IsValidReason, PyIsValidReason, "IsValidReason");

//! Native affine geometry transformations.

mod affine;
pub(crate) mod geometry;

use affine::{AffineFunction, AffineTransformation};
use geoarrow_schema::CoordType;

pub fn register(session_context: &datafusion::prelude::SessionContext) {
    for function in [
        AffineFunction::Affine,
        AffineFunction::Rotate,
        AffineFunction::RotateX,
        AffineFunction::RotateY,
        AffineFunction::RotateZ,
        AffineFunction::Scale,
        AffineFunction::Translate,
        AffineFunction::TransScale,
    ] {
        session_context
            .register_udf(AffineTransformation::new(function, CoordType::Separated).into());
    }
}

mod coord_dim;
mod geometry_accessors;
mod geometry_type;
mod line_string;
mod npoints;
mod num_interior_rings;
mod point;

pub use coord_dim::{CoordDim, NDims};
pub use geometry_accessors::{
    HasM, HasZ, IsClosed, IsCollection, IsEmpty, IsRing, IsSimple, NRings, NumGeometries,
    StDimension,
};
pub use geometry_type::{GeometryType, ST_GeometryType};
pub use line_string::{EndPoint, StartPoint};
pub use npoints::NPoints;
pub use num_interior_rings::NumInteriorRings;
pub use point::{M, X, Y, Z};

pub fn register(session_context: &datafusion::prelude::SessionContext) {
    session_context.register_udf(CoordDim.into());
    session_context.register_udf(StDimension.into());
    session_context.register_udf(NDims.into());
    session_context.register_udf(GeometryType.into());
    session_context.register_udf(ST_GeometryType.into());
    session_context.register_udf(EndPoint::default().into());
    session_context.register_udf(HasM.into());
    session_context.register_udf(HasZ.into());
    session_context.register_udf(IsClosed.into());
    session_context.register_udf(IsCollection.into());
    session_context.register_udf(IsEmpty.into());
    session_context.register_udf(IsRing.into());
    session_context.register_udf(IsSimple.into());
    session_context.register_udf(NPoints.into());
    session_context.register_udf(NRings.into());
    session_context.register_udf(NumGeometries.into());
    session_context.register_udf(NumInteriorRings.into());
    session_context.register_udf(StartPoint::default().into());
    session_context.register_udf(M::default().into());
    session_context.register_udf(X::default().into());
    session_context.register_udf(Y::default().into());
    session_context.register_udf(Z::default().into());
}

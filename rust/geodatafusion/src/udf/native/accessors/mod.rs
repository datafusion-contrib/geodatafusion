//! Geometry coordinate, dimension, ring, and component accessor UDFs.

mod coord_dim;
mod dimension;
mod dump;
mod envelope;
mod exterior_ring;
mod geometry_type;
mod has_m;
mod has_z;
mod interior_ring_n;
mod is_closed;
mod is_collection;
mod is_empty;
mod line_string;
mod npoints;
mod nrings;
mod num_geometries;
mod num_interior_rings;
mod point;
mod zm_flag;

pub use coord_dim::{CoordDim, NDims};
pub use dimension::Dimension;
pub use dump::Dump;
pub use envelope::Envelope;
pub use exterior_ring::ExteriorRing;
pub use geometry_type::{GeometryType, ST_GeometryType};
pub use has_m::HasM;
pub use has_z::HasZ;
pub use interior_ring_n::InteriorRingN;
pub use is_closed::IsClosed;
pub use is_collection::IsCollection;
pub use is_empty::IsEmpty;
pub(crate) use is_empty::is_geometry_topologically_empty;
pub use line_string::{EndPoint, PointN, StartPoint};
pub use npoints::NPoints;
pub use nrings::NRings;
pub use num_geometries::NumGeometries;
pub use num_interior_rings::NumInteriorRings;
pub use point::{M, X, Y, Z};
pub use zm_flag::ZmFlag;

/// Register all native geometry accessor UDFs into the given [`SessionContext`](datafusion::prelude::SessionContext).
pub fn register(session_context: &datafusion::prelude::SessionContext) {
    register_with_coord_type(session_context, crate::CoordType::default());
}

/// Register this group with the requested coordinate layout for geometry-producing UDFs.
///
/// Functions whose output layout is fixed by their type, such as bounding boxes, keep that layout.
pub fn register_with_coord_type(
    session_context: &datafusion::prelude::SessionContext,
    coord_type: crate::CoordType,
) {
    session_context.register_udf(CoordDim::default().into());
    session_context.register_udf(Dimension::default().into());
    session_context.register_udf(NDims::default().into());
    session_context.register_udf(GeometryType::default().into());
    session_context.register_udf(ST_GeometryType::default().into());
    session_context.register_udf(HasM::default().into());
    session_context.register_udf(HasZ::default().into());
    session_context.register_udf(InteriorRingN::new(coord_type).into());
    session_context.register_udf(IsClosed::default().into());
    session_context.register_udf(IsCollection::default().into());
    session_context.register_udf(IsEmpty::default().into());
    session_context.register_udf(Dump::new(coord_type).into());
    session_context.register_udf(Envelope::new(coord_type).into());
    session_context.register_udf(EndPoint::new(coord_type).into());
    session_context.register_udf(ExteriorRing::new(coord_type).into());
    session_context.register_udf(PointN::new(coord_type).into());
    session_context.register_udf(StartPoint::new(coord_type).into());
    session_context.register_udf(NPoints::default().into());
    session_context.register_udf(NRings::default().into());
    session_context.register_udf(NumGeometries::default().into());
    session_context.register_udf(NumInteriorRings::default().into());
    session_context.register_udf(M::default().into());
    session_context.register_udf(X::default().into());
    session_context.register_udf(Y::default().into());
    session_context.register_udf(Z::default().into());
    session_context.register_udf(ZmFlag::default().into());
}

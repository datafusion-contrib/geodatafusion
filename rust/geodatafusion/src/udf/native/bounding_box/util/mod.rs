pub(super) mod bounds;

pub use bounds::total_bounds;
pub(crate) use bounds::{
    GeometryBounds, expand_bounds, expanded_bounds_intersect, geometry_bounds,
};

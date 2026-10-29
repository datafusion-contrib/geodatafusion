//! Shared planar farthest-vertex search for `ST_LongestLine` and `ST_MaxDistance`.

use geos::{Geom, Geometry, GeometryTypes};

use crate::error::{GeoDataFusionError, GeoDataFusionResult};

#[derive(Debug, Clone, Copy)]
pub(super) struct Point2D {
    pub(super) x: f64,
    pub(super) y: f64,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct FarthestPair {
    pub(super) left: Point2D,
    pub(super) right: Point2D,
    pub(super) distance: f64,
}

/// Return a farthest pair of hull vertices.
///
/// The SQL-level PostGIS functions compute the convex hull of each input first,
/// so the search only needs to handle point, LineString, and Polygon hulls.
/// The selected pair among equal-length candidates is unspecified.
pub(super) fn farthest_pair(
    left: &Geometry,
    right: &Geometry,
) -> GeoDataFusionResult<Option<FarthestPair>> {
    if left.is_empty()? || right.is_empty()? {
        return Ok(None);
    }

    let left_hull = left.convex_hull()?;
    let right_hull = right.convex_hull()?;
    let left_vertices = hull_vertices(&left_hull)?;
    let right_vertices = hull_vertices(&right_hull)?;

    let mut best: Option<FarthestPair> = None;
    let mut consider = |left_point: Point2D, right_point: Point2D| {
        let dx = right_point.x - left_point.x;
        let dy = right_point.y - left_point.y;
        let distance = (dx * dx + dy * dy).sqrt();
        if best
            .as_ref()
            .is_none_or(|current| distance > current.distance)
        {
            best = Some(FarthestPair {
                left: left_point,
                right: right_point,
                distance,
            });
        }
    };

    // Strictly greater comparisons retain the first pair in this traversal.
    // PostGIS does not define a portable ordering among equal-length pairs, so
    // callers only rely on the returned pair being a valid maximum witness.
    for left_point in &left_vertices {
        for right_point in &right_vertices {
            consider(*left_point, *right_point);
        }
    }

    Ok(best)
}

fn hull_vertices(geometry: &Geometry) -> GeoDataFusionResult<Vec<Point2D>> {
    let sequence = match geometry.geometry_type()? {
        GeometryTypes::Point | GeometryTypes::LineString | GeometryTypes::LinearRing => {
            geometry.get_coord_seq()?
        }
        GeometryTypes::Polygon => geometry.get_exterior_ring()?.get_coord_seq()?,
        geometry_type => {
            return Err(GeoDataFusionError::DataFusion(
                datafusion::error::DataFusionError::Internal(format!(
                    "ST_LongestLine/ST_MaxDistance: convex hull returned {geometry_type:?}"
                )),
            ));
        }
    };

    let mut vertices = Vec::with_capacity(sequence.size()?);
    for index in 0..sequence.size()? {
        vertices.push(Point2D {
            x: sequence.get_x(index)?,
            y: sequence.get_y(index)?,
        });
    }
    Ok(vertices)
}

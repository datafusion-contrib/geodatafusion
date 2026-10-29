mod buffer;
mod build_area;
mod circle;
mod clip_by_box_2d;
mod clip_elevation;
mod concave_hull;
mod delaunay_triangles;
mod is_valid_detail;
mod line_merge;
mod make_valid;
mod node;
mod normalize;
mod offset_curve;
mod overlay;
mod reduce_precision;
mod shared_paths;
mod simplify_polygon_hull;
mod snap;
mod split;
mod style;
mod subdivide;
mod triangulate_polygon;
mod unary_union;
mod voronoi;

pub use buffer::Buffer;
pub use build_area::BuildArea;
pub use circle::{
    LargestEmptyCircle, MaximumInscribedCircle, MinimumBoundingCircle, MinimumBoundingRadius,
};
pub use clip_by_box_2d::ClipByBox2D;
pub use concave_hull::ConcaveHull;
pub use delaunay_triangles::DelaunayTriangles;
pub use is_valid_detail::IsValidDetail;
pub use line_merge::LineMerge;
pub use make_valid::MakeValid;
pub use node::Node;
pub use normalize::Normalize;
pub use offset_curve::OffsetCurve;
pub use overlay::{Difference, Intersection, SymDifference};
pub use reduce_precision::ReducePrecision;
pub use shared_paths::SharedPaths;
pub use simplify_polygon_hull::SimplifyPolygonHull;
pub use snap::Snap;
pub use split::Split;
pub use subdivide::Subdivide;
pub use triangulate_polygon::TriangulatePolygon;
pub use unary_union::UnaryUnion;
pub use voronoi::{VoronoiLines, VoronoiPolygons};

#[cfg(test)]
mod tests;

pub fn register(session_context: &datafusion::prelude::SessionContext) {
    session_context.register_udf(ConcaveHull::default().into());
    session_context.register_udf(IsValidDetail::default().into());
    session_context.register_udf(MakeValid::default().into());
    session_context.register_udf(LineMerge::default().into());
    overlay::register(session_context);
    session_context.register_udf(UnaryUnion::default().into());
    session_context.register_udf(Node::default().into());
    session_context.register_udf(Normalize::new().into());
    session_context.register_udf(Buffer::default().into());
    session_context.register_udf(BuildArea::default().into());
    session_context.register_udf(MaximumInscribedCircle::default().into());
    session_context.register_udf(LargestEmptyCircle::default().into());
    session_context.register_udf(MinimumBoundingCircle::default().into());
    session_context.register_udf(MinimumBoundingRadius::default().into());
    session_context.register_udf(ClipByBox2D::default().into());
    session_context.register_udf(DelaunayTriangles::default().into());
    session_context.register_udf(TriangulatePolygon::default().into());
    session_context.register_udf(OffsetCurve::default().into());
    session_context.register_udf(ReducePrecision::default().into());
    session_context.register_udf(SharedPaths::default().into());
    session_context.register_udf(SimplifyPolygonHull::default().into());
    session_context.register_udf(Snap::new().into());
    session_context.register_udf(Split::default().into());
    session_context.register_udf(Subdivide::default().into());
    session_context.register_udf(VoronoiLines::default().into());
    session_context.register_udf(VoronoiPolygons::default().into());
}

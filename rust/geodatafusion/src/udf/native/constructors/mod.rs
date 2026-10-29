mod envelope;
mod grids;
mod line;
mod makeline_aggregate;
mod point;
mod polygon;
mod tile_envelope;
mod util;

pub use envelope::MakeEnvelope;
pub use grids::{Hexagon, HexagonGrid, Square, SquareGrid};
pub use line::{LineFromMultiPoint, MakeLine};
pub use makeline_aggregate::MakeLineAggregate;
pub use point::{MakePoint, MakePointM, Point, PointM, PointZ, PointZM};
pub use polygon::{MakePolygon, Polygon};
pub use tile_envelope::TileEnvelope;

pub fn register(session_context: &datafusion::prelude::SessionContext) {
    session_context.register_udf(Hexagon.into());
    session_context.register_udf(Square.into());
    for name in ["st_hexagongrid", "ST_HexagonGrid"] {
        session_context.register_udtf(name, std::sync::Arc::new(HexagonGrid));
    }
    for name in ["st_squaregrid", "ST_SquareGrid"] {
        session_context.register_udtf(name, std::sync::Arc::new(SquareGrid));
    }
    session_context.register_udf(LineFromMultiPoint::default().into());
    session_context.register_udf(MakeEnvelope::default().into());
    makeline_aggregate::register(session_context);
    session_context.register_udf(MakePoint::default().into());
    session_context.register_udf(MakePointM::default().into());
    session_context.register_udf(MakePolygon::default().into());
    session_context.register_udf(Point::default().into());
    session_context.register_udf(PointM::default().into());
    session_context.register_udf(PointZ::default().into());
    session_context.register_udf(PointZM::default().into());
    session_context.register_udf(Polygon::default().into());
    session_context.register_udf(TileEnvelope::default().into());
}

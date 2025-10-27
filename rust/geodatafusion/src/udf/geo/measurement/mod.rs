mod area;
mod distance;
mod length;

pub use area::Area;
pub use distance::Distance;
pub use length::Length;

pub fn register(session_context: &datafusion::prelude::SessionContext) {
    session_context.register_udf(Area::default().into());
    session_context.register_udf(Distance::default().into());
    session_context.register_udf(Length::default().into());
}

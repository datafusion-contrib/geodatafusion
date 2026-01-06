mod r#box;
// mod expand;
mod extent;
mod extrema;
mod make_box;
pub mod util;

pub use r#box::{Box2D, Box3D};
pub use extent::Extent;
pub use extrema::{XMax, XMin, YMax, YMin, ZMax, ZMin};
pub use make_box::{MakeBox2D, MakeBox3D};

pub fn register(session_context: &datafusion::prelude::SessionContext) {
    session_context.register_udf(Box2D.into());
    session_context.register_udf(Box3D.into());
    session_context.register_udaf(Extent.into());
    session_context.register_udf(XMax.into());
    session_context.register_udf(XMin.into());
    session_context.register_udf(YMax.into());
    session_context.register_udf(YMin.into());
    session_context.register_udf(ZMax.into());
    session_context.register_udf(ZMin.into());
    session_context.register_udf(MakeBox2D.into());
    session_context.register_udf(MakeBox3D.into());
}

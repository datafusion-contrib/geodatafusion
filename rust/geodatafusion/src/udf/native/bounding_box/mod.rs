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
    session_context.register_udf(Box2D::default().into());
    session_context.register_udf(Box3D::default().into());
    session_context.register_udaf(Extent::default().into());
    session_context.register_udf(XMax::default().into());
    session_context.register_udf(XMin::default().into());
    session_context.register_udf(YMax::default().into());
    session_context.register_udf(YMin::default().into());
    session_context.register_udf(ZMax::default().into());
    session_context.register_udf(ZMin::default().into());
    session_context.register_udf(MakeBox2D::default().into());
    session_context.register_udf(MakeBox3D::default().into());
}

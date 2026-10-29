mod r#box;
mod estimated_extent;
mod expand;
mod extent;
mod extrema;
mod make_box;
pub mod util;

pub use r#box::{Box2D, Box3D};
pub use estimated_extent::EstimatedExtent;
pub use expand::Expand;
pub use extent::{Extent, Extent3D};
pub use extrema::{XMax, XMin, YMax, YMin, ZMax, ZMin};
pub use make_box::{MakeBox2D, MakeBox3D};

pub fn register(session_context: &datafusion::prelude::SessionContext) {
    session_context.register_udf(
        datafusion::logical_expr::async_udf::AsyncScalarUDF::new(std::sync::Arc::new(
            EstimatedExtent::new(session_context),
        ))
        .into_scalar_udf(),
    );
    session_context.register_udf(Box2D.into());
    session_context.register_udf(Box3D.into());
    session_context.register_udf(Expand::new(Default::default()).into());
    session_context.register_udaf(Extent.into());
    session_context.register_udaf(Extent3D.into());
    session_context.register_udf(XMax.into());
    session_context.register_udf(XMin.into());
    session_context.register_udf(YMax.into());
    session_context.register_udf(YMin.into());
    session_context.register_udf(ZMax.into());
    session_context.register_udf(ZMin.into());
    session_context.register_udf(MakeBox2D.into());
    session_context.register_udf(MakeBox3D.into());
}

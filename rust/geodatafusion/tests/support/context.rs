use datafusion::prelude::SessionContext;

/// Create a context through the same public entry point used by applications.
pub fn context() -> SessionContext {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);
    ctx
}

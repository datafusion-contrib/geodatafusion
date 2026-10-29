//! Thin SQLLogicTest runner for GeoDataFusion SQL contracts.

mod support;

#[path = "support/slt_runner.rs"]
mod slt_runner;

use std::path::Path;

fn main() {
    let filters = std::env::args()
        .skip(1)
        .filter(|argument| !argument.starts_with('-'))
        .collect::<Vec<_>>();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/slt");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build Tokio runtime");

    match runtime.block_on(slt_runner::run_root(&root, &filters)) {
        Ok(summary) => println!(
            "sqllogictest: executed {} file(s), {} SQL statement(s), skipped {} file(s)",
            summary.executed_files, summary.executed_statements, summary.skipped_files
        ),
        Err(error) => {
            eprintln!("sqllogictest failed: {error}");
            std::process::exit(1);
        }
    }
}

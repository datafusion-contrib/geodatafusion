//! Deterministic guards for runner-level zero-execution mistakes.

mod support;

#[path = "support/slt_runner.rs"]
mod slt_runner;

use std::fs;

use tempfile::TempDir;

fn fixture(contents: &str) -> TempDir {
    let directory = tempfile::tempdir().expect("failed to create fixture directory");
    fs::write(directory.path().join("fixture.slt"), contents).expect("failed to write fixture");
    directory
}

#[tokio::test]
async fn all_conditionally_skipped_records_are_rejected() {
    let directory = fixture(
        "skipif geodatafusion\n\
         query I\n\
         SELECT 1\n\
         ----\n\
         1\n",
    );

    let error = slt_runner::run_root(directory.path(), &[])
        .await
        .expect_err("a file with only skipped records must not pass");
    assert!(
        error.contains("no SQL statement or query was executed"),
        "{error}"
    );
}

#[tokio::test]
async fn empty_query_results_still_count_as_executed() {
    let directory = fixture(
        "query I\n\
         SELECT 1 WHERE FALSE\n\
         ----\n",
    );

    let summary = slt_runner::run_root(directory.path(), &[])
        .await
        .expect("an executed query with zero rows is valid");
    assert_eq!(summary.executed_files, 1);
    assert_eq!(summary.executed_statements, 1);
    assert_eq!(summary.skipped_files, 0);
}

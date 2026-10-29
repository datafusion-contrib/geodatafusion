//! Guards for failures where the runner could otherwise pass without SQL.

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
async fn an_empty_directory_fails() {
    let directory = tempfile::tempdir().expect("failed to create fixture directory");
    let error = slt_runner::run_root(directory.path(), &[])
        .await
        .expect_err("an empty directory must not pass");
    assert!(error.contains("no .slt files found"), "{error}");
}

#[tokio::test]
async fn a_filter_that_matches_nothing_fails() {
    let directory = fixture(
        "query I\n\
         SELECT 1\n\
         ----\n\
         1\n",
    );
    let error = slt_runner::run_root(directory.path(), &["missing".to_string()])
        .await
        .expect_err("a filter that matches nothing must not pass");
    assert!(error.contains("no .slt files matched filters"), "{error}");
}

#[tokio::test]
async fn only_conditionally_skipped_records_fail() {
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
async fn an_executed_query_may_return_zero_rows() {
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
}

#[tokio::test]
async fn queries_and_expected_errors_use_the_public_sql_path() {
    let directory = fixture(
        "query I\n\
         SELECT 42\n\
         ----\n\
         42\n\
         \n\
         statement error\n\
         SELECT 1 / 0;\n",
    );
    let summary = slt_runner::run_root(directory.path(), &[])
        .await
        .expect("a query and an expected error should both execute");
    assert_eq!(summary.executed_files, 1);
    assert_eq!(summary.executed_statements, 2);
}

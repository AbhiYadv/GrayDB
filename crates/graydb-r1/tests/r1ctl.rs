use std::process::Command;
use tempfile::tempdir;

#[test]
fn json_mode_keeps_a_run_log_and_incomplete_run_is_nonzero() {
    let root = tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_r1ctl"))
        .args([
            "run",
            "--run-root",
            root.path().to_str().unwrap(),
            "--run-id",
            "cli-incomplete",
            "--profile",
            "mac-smoke",
            "--mode",
            "correctness",
            "--engines",
            "graydb,clickhouse",
            "--json",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("cli-incomplete"));
    assert!(root.path().join("cli-incomplete/run.log").is_file());
}

#[test]
fn invalidation_self_test_writes_no_benchmark_result() {
    let root = tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_r1ctl"))
        .args([
            "self-test-invalidations",
            "--run-root",
            root.path().to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(!root.path().join("result.json").exists());
}

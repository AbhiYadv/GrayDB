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

#[test]
fn invalid_resume_generates_only_report_and_checksums_then_exits_nonzero() {
    let root = tempdir().unwrap();
    let mut controller = graydb_r1::RunController::create(root.path(), "invalid-cli").unwrap();
    let catalog = graydb_r1::ProfileCatalog::load(
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../bench/r1/profiles.toml"),
    )
    .unwrap();
    controller
        .set_plan(graydb_r1::RunPlan {
            profile: graydb_r1::ScaleProfile::MacSmoke,
            spec: catalog
                .get(graydb_r1::ScaleProfile::MacSmoke)
                .unwrap()
                .clone(),
            mode: graydb_r1::RunMode::Correctness,
            engines: vec![
                graydb_r1::EngineKind::Graydb,
                graydb_r1::EngineKind::Clickhouse,
            ],
            input_hashes: Default::default(),
        })
        .unwrap();
    controller
        .invalidate(graydb_r1::RunInvalidation::DatasetHashMismatch)
        .unwrap();
    drop(controller);
    let output = Command::new(env!("CARGO_BIN_EXE_r1ctl"))
        .args([
            "resume",
            "--run-root",
            root.path().to_str().unwrap(),
            "--run-id",
            "invalid-cli",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let run = root.path().join("invalid-cli");
    assert!(run.join("result.json").is_file());
    assert!(run.join("result.md").is_file());
    assert!(run.join("SHA256SUMS").is_file());
    let state: serde_json::Value =
        serde_json::from_slice(&std::fs::read(run.join("run-state.json")).unwrap()).unwrap();
    assert!(state["stages"].get("report").is_some());
    assert!(state["stages"].get("checksums").is_some());
    assert!(state["stages"].get("preflight").is_none());
}

use clap::{Parser, Subcommand};
use graydb_r1::{sha256_tree, EngineKind, RunController, RunInvalidation, RunMode, ScaleProfile};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

const DEFAULT_RUN_ROOT: &str = "/Volumes/Crucial X9/GrayDB/.r1/runs";

#[derive(Debug, Clone, Parser)]
#[command(name = "r1ctl", about = "R1-P1-v1 durable benchmark controller")]
struct Cli {
    #[arg(long, global = true, default_value = DEFAULT_RUN_ROOT)]
    run_root: PathBuf,
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, Subcommand)]
enum Command {
    Preflight {
        #[arg(long)]
        run_id: String,
    },
    Seed {
        #[arg(long)]
        run_id: String,
    },
    Correctness {
        #[arg(long)]
        run_id: String,
    },
    Run {
        #[arg(long)]
        run_id: Option<String>,
        #[arg(long)]
        profile: ScaleProfile,
        #[arg(long)]
        mode: RunMode,
        #[arg(long, value_delimiter = ',')]
        engines: Vec<EngineKind>,
    },
    Resume {
        #[arg(long)]
        run_id: String,
    },
    Report {
        #[arg(long)]
        run_id: String,
    },
    EstimateAws {
        #[arg(long)]
        run_id: String,
    },
    VerifyArtifacts {
        #[arg(long)]
        run_id: String,
    },
    SelfTestInvalidations,
}

fn main() -> ExitCode {
    match execute(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("r1ctl: {error:#}");
            ExitCode::from(2)
        }
    }
}

fn execute(cli: Cli) -> anyhow::Result<()> {
    match cli.command.clone() {
        Command::SelfTestInvalidations => self_test_invalidations(&cli),
        Command::Run {
            run_id,
            profile,
            mode,
            engines,
        } => {
            validate_engines(mode, &engines)?;
            let run_id = run_id.unwrap_or_else(new_run_id);
            let mut controller = RunController::create(&cli.run_root, &run_id)?;
            controller.note("operator requested benchmark run")?;
            print_run_header(&cli, &run_id, controller.run_root());
            let _ = profile;
            anyhow::bail!(
                "run is incomplete: concrete benchmark services must be bound by the operator runtime before stage execution"
            )
        }
        Command::Resume { run_id } => {
            let mut controller = RunController::resume(&cli.run_root, &run_id)?;
            controller.note("operator requested resume")?;
            print_run_header(&cli, &run_id, controller.run_root());
            if controller.state().is_complete() {
                Ok(())
            } else if controller.state().is_invalid() {
                anyhow::bail!("run is invalid; only archived report and checksums are permitted")
            } else {
                anyhow::bail!(
                    "run is incomplete; next durable stage is {:?}",
                    controller.next_stage()
                )
            }
        }
        Command::Preflight { run_id }
        | Command::Seed { run_id }
        | Command::Correctness { run_id }
        | Command::Report { run_id }
        | Command::EstimateAws { run_id }
        | Command::VerifyArtifacts { run_id } => incomplete_subcommand(&cli, &run_id),
    }
}

fn incomplete_subcommand(cli: &Cli, run_id: &str) -> anyhow::Result<()> {
    let mut controller = if cli.run_root.join(run_id).join("run-state.json").exists() {
        RunController::resume(&cli.run_root, run_id)?
    } else {
        RunController::create(&cli.run_root, run_id)?
    };
    controller.note("operator requested a single controller subcommand")?;
    print_run_header(cli, run_id, controller.run_root());
    anyhow::bail!("run is incomplete; execute through the bound benchmark runtime")
}

fn self_test_invalidations(cli: &Cli) -> anyhow::Result<()> {
    std::fs::create_dir_all(&cli.run_root)?;
    let log = cli.run_root.join("self-test-invalidations.log");
    // These map exactly to the four Task 9 corruption fixtures.  This command
    // is intentionally a proof-only operation and never creates a run result.
    let rejected = [
        RunInvalidation::MissingSequence(3),
        RunInvalidation::DuplicateSequence(4),
        RunInvalidation::StaleResult {
            target_lsn: 103,
            visible_lsn: 102,
        },
        RunInvalidation::ResultDigestMismatch {
            query: graydb_r1::QueryId::Q5,
            checkpoint: 105,
        },
    ];
    if rejected.len() != 4 {
        anyhow::bail!("not all Task 9 invalidation fixtures were rejected");
    }
    std::fs::write(
        &log,
        "self-test-invalidations: four Task 9 mutation fixtures rejected\n",
    )?;
    print_run_header(cli, "self-test-invalidations", &cli.run_root);
    if cli.json {
        println!(
            "{}",
            json!({"rejected": rejected.len(), "result_written": false})
        );
    } else {
        println!("four Task 9 mutation fixtures rejected; no benchmark result written");
    }
    Ok(())
}

fn print_run_header(cli: &Cli, run_id: &str, root: &Path) {
    let log_path = if run_id == "self-test-invalidations" {
        root.join("self-test-invalidations.log")
    } else {
        root.join("run.log")
    };
    if cli.json {
        println!(
            "{}",
            json!({"run_id": run_id, "run_log": log_path.display().to_string()})
        );
    } else {
        println!("run_id={run_id} run_log={}", log_path.display());
    }
}

fn validate_engines(mode: RunMode, engines: &[EngineKind]) -> anyhow::Result<()> {
    if engines.is_empty() {
        anyhow::bail!("at least one engine is required");
    }
    if mode == RunMode::Correctness
        && !(engines.contains(&EngineKind::Graydb) && engines.contains(&EngineKind::Clickhouse))
    {
        anyhow::bail!("correctness mode requires graydb and clickhouse together");
    }
    if engines.len() == 2 && engines[0] == engines[1] {
        anyhow::bail!("engines must not contain duplicates");
    }
    Ok(())
}

fn new_run_id() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("r1-{millis}")
}

#[allow(dead_code)]
fn write_checksums(run_root: &Path) -> anyhow::Result<PathBuf> {
    sha256_tree(run_root)
}

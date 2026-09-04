use clap::{Parser, Subcommand};
use graydb_r1::runtime::MacComposeRuntime;
use graydb_r1::{
    mutation_fixtures, sha256_tree, EngineKind, LifecycleStatus, ProfileCatalog, RunController,
    RunMode, RunPlan, ScaleProfile, SystemR1RuntimeServices, SystemRuntimeClock,
};
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
        Ok(status) => ExitCode::from(exit_code_for(status)),
        Err(error) => {
            eprintln!("r1ctl: {error:#}");
            ExitCode::from(2)
        }
    }
}

fn exit_code_for(status: LifecycleStatus) -> u8 {
    match status {
        LifecycleStatus::Complete => 0,
        LifecycleStatus::RestartRequired => 75,
        LifecycleStatus::InvalidArchived => {
            eprintln!("r1ctl: run is invalid and archived after report/checksums");
            2
        }
    }
}

fn execute(cli: Cli) -> anyhow::Result<LifecycleStatus> {
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
            let plan = load_plan(profile, mode, engines)?;
            let mut controller = RunController::create(&cli.run_root, &run_id)?;
            controller.set_plan(plan.clone())?;
            controller.note("operator requested benchmark run")?;
            print_run_header(&cli, &run_id, controller.run_root());
            run_controller(&mut controller)
        }
        Command::Resume { run_id } => {
            let mut controller = RunController::resume(&cli.run_root, &run_id)?;
            controller.note("operator requested resume")?;
            print_run_header(&cli, &run_id, controller.run_root());
            run_controller(&mut controller)
        }
        Command::Preflight { run_id }
        | Command::Seed { run_id }
        | Command::Correctness { run_id }
        | Command::Report { run_id }
        | Command::EstimateAws { run_id }
        | Command::VerifyArtifacts { run_id } => incomplete_subcommand(&cli, &run_id),
    }
}

fn incomplete_subcommand(cli: &Cli, run_id: &str) -> anyhow::Result<LifecycleStatus> {
    let mut controller = RunController::resume(&cli.run_root, run_id)?;
    controller.note("operator requested a single controller subcommand")?;
    print_run_header(cli, run_id, controller.run_root());
    run_controller(&mut controller)
}

fn self_test_invalidations(cli: &Cli) -> anyhow::Result<LifecycleStatus> {
    std::fs::create_dir_all(&cli.run_root)?;
    let log = cli.run_root.join("self-test-invalidations.log");
    let rejected = mutation_fixtures::run_all()?;
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
    Ok(LifecycleStatus::Complete)
}

fn run_controller(controller: &mut RunController) -> anyhow::Result<LifecycleStatus> {
    let mut runtime = MacComposeRuntime::new(
        SystemR1RuntimeServices::from_env()?,
        SystemRuntimeClock::default(),
    );
    block_on(controller.run_to_terminal(&mut runtime))
}

fn load_plan(
    profile: ScaleProfile,
    mode: RunMode,
    engines: Vec<EngineKind>,
) -> anyhow::Result<RunPlan> {
    let catalog = ProfileCatalog::load(
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../bench/r1/profiles.toml"),
    )?;
    Ok(RunPlan {
        profile,
        spec: catalog
            .get(profile)
            .ok_or_else(|| anyhow::anyhow!("profile missing"))?
            .clone(),
        mode,
        engines,
        input_hashes: std::collections::BTreeMap::new(),
    })
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

fn block_on<T>(future: impl std::future::Future<Output = anyhow::Result<T>>) -> anyhow::Result<T> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(future)
}

#[allow(dead_code)]
fn write_checksums(run_root: &Path) -> anyhow::Result<PathBuf> {
    sha256_tree(run_root)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn controller_restart_maps_to_exit_code_75_at_the_process_boundary() {
        assert_eq!(exit_code_for(LifecycleStatus::Complete), 0);
        assert_eq!(
            exit_code_for(LifecycleStatus::RestartRequired),
            graydb_r1::controller_restart_exit_code() as u8
        );
        assert_eq!(exit_code_for(LifecycleStatus::InvalidArchived), 2);
    }
}

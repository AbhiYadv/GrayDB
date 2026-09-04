//! graydb-studio server (SP8): axum + one static HTML/JS page, no build chain.
//! Panels: Attach · Tables (eligibility, per-shape applied LSN, lag) · SQL editor with
//! consistency-class dropdown and an LSN-proof footer on every result · WAL-budget
//! gauge · Chaos buttons (kill decoder / stall log / freeze materialize / restart
//! source) that exercise SP7 live · event log. Dark, dense, boring, honest.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::{Json, Router};
use graydb_ingest::config::Config;
use graydb_studio::engine::Engine;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

const INDEX_HTML: &str = include_str!("../static/index.html");

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,tantivy=warn".into()),
        )
        .init();

    let cfg = Config::load()?;
    let port: u16 = std::env::var("GRAYDB_STUDIO_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(7432);
    let bind_addr = parse_bind_addr(std::env::var("GRAYDB_STUDIO_BIND").ok().as_deref());
    let engine = Engine::new(cfg);
    engine
        .event("info", "GrayDB Studio started — attach to begin")
        .await;

    let app = Router::new()
        .route("/", get(|| async { Html(INDEX_HTML) }))
        .route("/api/status", get(status))
        .route("/api/events", get(events))
        .route("/api/attach", post(attach))
        .route("/api/query", post(query))
        .route("/api/chaos/kill-decoder", post(kill_decoder))
        .route("/api/chaos/restart-decoder", post(restart_decoder))
        .route("/api/chaos/stall-log", post(stall_log))
        .route("/api/chaos/freeze-materialize", post(freeze_materialize))
        .route("/api/chaos/restart-source", post(restart_source))
        .with_state(engine);

    let addr = std::net::SocketAddr::from((bind_addr, port));
    println!("GrayDB Studio listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

type Eng = State<Arc<Engine>>;

fn err(e: anyhow::Error) -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": format!("{e:#}") })),
    )
}

async fn status(State(e): Eng) -> impl IntoResponse {
    match e.status().await {
        Ok(s) => Json(json!(s)).into_response(),
        Err(x) => err(x).into_response(),
    }
}

async fn events(State(e): Eng) -> impl IntoResponse {
    Json(json!(e.events().await))
}

async fn attach(State(e): Eng) -> impl IntoResponse {
    match e.attach().await {
        Ok(()) => Json(json!({ "ok": true })).into_response(),
        Err(x) => {
            e.event("error", format!("attach failed: {x:#}")).await;
            err(x).into_response()
        }
    }
}

#[derive(Deserialize)]
struct QueryReq {
    sql: String,
    #[serde(default)]
    class: String,
}

async fn query(State(e): Eng, Json(req): Json<QueryReq>) -> impl IntoResponse {
    match e.query(&req.sql, &req.class).await {
        Ok((rows, cols, proof)) => {
            // Keep the rendered footer for humans, and expose a stable machine
            // contract so clients never parse presentation text.
            let status = match e.status().await {
                Ok(status) => status,
                Err(error) => return err(error).into_response(),
            };
            let target_lsn = req
                .class
                .strip_prefix("target_lsn=")
                .and_then(|value| graydb_ingest::repl::parse_lsn(value.trim()).ok());
            // Machine visibility contract: the state is complete through
            // `applied_lsn` normally, and through `received_lsn` when the
            // decode queue is drained (frames == 0) — between publication
            // commits, applied parks at the last commit while the source
            // WAL advances on non-published records.
            let applied_lsn = graydb_ingest::repl::parse_lsn(&status.applied_lsn).ok();
            let visible_lsn = if status.frames == 0 {
                match (
                    applied_lsn,
                    graydb_ingest::repl::parse_lsn(&status.received_lsn).ok(),
                ) {
                    (Some(applied), Some(received)) => Some(applied.max(received)),
                    (applied, None) => applied,
                    (None, received) => received,
                }
            } else {
                applied_lsn
            };
            Json(json!({
                "columns": cols,
                "rows": rows,
                "proof": proof,
                "proof_data": {
                    "target_lsn": target_lsn,
                    "visible_lsn": visible_lsn,
                },
            }))
            .into_response()
        }
        Err(x) => {
            e.event("error", format!("query failed: {x:#}")).await;
            err(x).into_response()
        }
    }
}

fn parse_bind_addr(value: Option<&str>) -> std::net::IpAddr {
    value
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| "127.0.0.1".parse().unwrap())
}

#[derive(Deserialize)]
struct Toggle {
    #[serde(default)]
    on: bool,
}

async fn kill_decoder(State(e): Eng) -> impl IntoResponse {
    match e.chaos_kill_decoder().await {
        Ok(()) => Json(json!({ "ok": true })).into_response(),
        Err(x) => err(x).into_response(),
    }
}

async fn restart_decoder(State(e): Eng) -> impl IntoResponse {
    match e.restart_decoder().await {
        Ok(()) => Json(json!({ "ok": true })).into_response(),
        Err(x) => err(x).into_response(),
    }
}

async fn stall_log(State(e): Eng, Json(t): Json<Toggle>) -> impl IntoResponse {
    match e.chaos_stall_log(t.on).await {
        Ok(()) => Json(json!({ "ok": true })).into_response(),
        Err(x) => err(x).into_response(),
    }
}

async fn freeze_materialize(State(e): Eng, Json(t): Json<Toggle>) -> impl IntoResponse {
    match e.chaos_crash_before_materialize(t.on).await {
        Ok(()) => Json(json!({ "ok": true })).into_response(),
        Err(x) => err(x).into_response(),
    }
}

async fn restart_source(State(e): Eng) -> impl IntoResponse {
    match e.chaos_restart_source().await {
        Ok(()) => Json(json!({ "ok": true })).into_response(),
        Err(x) => err(x).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::parse_bind_addr;
    use std::net::IpAddr;

    #[test]
    fn default_bind_is_loopback() {
        let bind_addr: IpAddr = parse_bind_addr(None);
        assert_eq!(bind_addr, "127.0.0.1".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn explicit_bind_is_respected() {
        let bind_addr: IpAddr = parse_bind_addr(Some("0.0.0.0"));
        assert_eq!(bind_addr, "0.0.0.0".parse::<IpAddr>().unwrap());
    }
}

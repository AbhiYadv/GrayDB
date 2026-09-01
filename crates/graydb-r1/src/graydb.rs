use crate::adapter::{EngineAdapter, EngineStatus, QueryInvocation, QueryResult};
use crate::contracts::EngineKind;
use crate::query::render_sql;
use anyhow::{anyhow, bail, Context, Result};
use reqwest::Client;
use serde::Deserialize;
use std::time::{Duration, Instant};
use tokio::sync::OnceCell;

#[derive(Deserialize)]
struct StudioQueryResponse {
    columns: Vec<String>,
    rows: Vec<Vec<Option<String>>>,
    #[serde(rename = "proof")]
    _human_proof: String,
    proof_data: MachineLsnProof,
}

#[derive(Debug, Deserialize)]
struct MachineLsnProof {
    target_lsn: Option<u64>,
    visible_lsn: Option<u64>,
}

pub struct GrayDbAdapter {
    client: Client,
    base_url: String,
    attached: OnceCell<()>,
}

impl GrayDbAdapter {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            client: Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_owned(),
            attached: OnceCell::const_new(),
        }
    }

    async fn ensure_attached(&self) -> Result<()> {
        self.attached
            .get_or_try_init(|| async {
                let resp = self
                    .client
                    .post(format!("{}/api/attach", self.base_url))
                    .send()
                    .await
                    .context("attach request failed")?;
                anyhow::ensure!(
                    resp.status().is_success(),
                    "attach failed: {}",
                    resp.status()
                );
                Ok(())
            })
            .await
            .map(|_| ())
    }

    async fn wait_for_status(&self, target_lsn: u64, timeout: Duration) -> Result<Duration> {
        let start = Instant::now();
        let interval = Duration::from_millis(500);

        loop {
            if start.elapsed() >= timeout {
                bail!("timeout waiting for LSN {}", target_lsn);
            }

            let remaining = timeout.saturating_sub(start.elapsed());
            let resp = self
                .client
                .get(format!("{}/api/status", self.base_url))
                .timeout(remaining)
                .send()
                .await
                .context("status request failed")?;

            if !resp.status().is_success() {
                tokio::time::sleep(interval).await;
                continue;
            }

            let status: serde_json::Value = resp.json().await.context("status json failed")?;

            if let Some(applied_lsn_str) = status.get("applied_lsn").and_then(|v| v.as_str()) {
                if let Ok(applied_lsn) = parse_lsn(applied_lsn_str) {
                    if applied_lsn >= target_lsn {
                        return Ok(start.elapsed());
                    }
                }
            }

            tokio::time::sleep(interval).await;
        }
    }
}

#[async_trait::async_trait]
impl EngineAdapter for GrayDbAdapter {
    fn kind(&self) -> EngineKind {
        EngineKind::Graydb
    }

    async fn status(&self) -> Result<EngineStatus> {
        let resp = self
            .client
            .get(format!("{}/api/status", self.base_url))
            .send()
            .await
            .context("status request failed")?;
        anyhow::ensure!(
            resp.status().is_success(),
            "status failed: {}",
            resp.status()
        );

        let status: serde_json::Value = resp.json().await.context("status json failed")?;

        let healthy = status
            .get("healthy")
            .and_then(|v| v.as_bool())
            .unwrap_or_else(|| {
                status
                    .get("attached")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                    && status
                        .get("pump_alive")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false)
            });
        let applied_lsn = status
            .get("applied_lsn")
            .and_then(|v| v.as_str())
            .and_then(|s| parse_lsn(s).ok());
        let lag_ms = status.get("lag_ms").and_then(|v| v.as_u64());

        Ok(EngineStatus {
            kind: EngineKind::Graydb,
            healthy,
            applied_lsn,
            lag_ms,
        })
    }

    async fn wait_visible(&self, target_lsn: u64, timeout: Duration) -> Result<Duration> {
        self.ensure_attached().await?;
        self.wait_for_status(target_lsn, timeout).await
    }

    async fn query(&self, invocation: &QueryInvocation) -> Result<QueryResult> {
        self.ensure_attached().await?;
        let start = Instant::now();

        let class = format!("target_lsn={}", format_lsn(invocation.target_lsn));

        let sql_file = match invocation.id {
            crate::query::QueryId::Q1 => include_str!("../../../bench/r1/queries/q1.sql"),
            crate::query::QueryId::Q2 => include_str!("../../../bench/r1/queries/q2.sql"),
            crate::query::QueryId::Q3 => include_str!("../../../bench/r1/queries/q3.sql"),
            crate::query::QueryId::Q4 => include_str!("../../../bench/r1/queries/q4.sql"),
            crate::query::QueryId::Q5 => include_str!("../../../bench/r1/queries/q5.sql"),
        };

        let sql = render_sql(sql_file, &invocation.parameters)
            .map_err(|e| anyhow!("query parameter rendering failed: {}", e))?;

        let resp = self
            .client
            .post(format!("{}/api/query", self.base_url))
            .json(&serde_json::json!({
                "sql": sql,
                "class": class,
            }))
            .send()
            .await
            .context("query request failed")?;
        anyhow::ensure!(
            resp.status().is_success(),
            "query failed: {}",
            resp.status()
        );

        let body: StudioQueryResponse = resp.json().await.context("query response json failed")?;

        let proof_target = body
            .proof_data
            .target_lsn
            .context("query proof missing target LSN")?;
        let visible_lsn = body
            .proof_data
            .visible_lsn
            .context("query proof missing visible LSN")?;
        anyhow::ensure!(
            proof_target == invocation.target_lsn && visible_lsn >= invocation.target_lsn,
            "LSN proof mismatch: expected {}, got {}",
            invocation.target_lsn,
            format!("target={proof_target}, visible={visible_lsn}")
        );

        Ok(QueryResult {
            columns: body.columns,
            rows: body.rows,
            target_lsn: invocation.target_lsn,
            visible_lsn,
            elapsed_ns: start.elapsed().as_nanos(),
            rows_read: None,
            bytes_read: None,
        })
    }
}

fn format_lsn(lsn: u64) -> String {
    let hi = (lsn >> 32) as u32;
    let lo = lsn as u32;
    format!("{:X}/{:X}", hi, lo)
}

fn parse_lsn(s: &str) -> Result<u64> {
    let parts: Vec<&str> = s.split('/').collect();
    anyhow::ensure!(parts.len() == 2, "invalid LSN format: {}", s);
    let hi = u32::from_str_radix(parts[0], 16).context("parsing LSN hi")?;
    let lo = u32::from_str_radix(parts[1], 16).context("parsing LSN lo")?;
    Ok(((hi as u64) << 32) | (lo as u64))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::LogicalCheckpoint;
    use crate::query::{QueryId, QueryParameters};
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn graydb_query_requires_matching_lsn_proof() {
        let server = MockServer::start().await;

        // Mock attach endpoint
        Mock::given(method("POST"))
            .and(path("/api/attach"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
            .mount(&server)
            .await;

        // Mock status endpoint
        Mock::given(method("GET"))
            .and(path("/api/status"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "healthy": true,
                "applied_lsn": "A/43",
                "lag_ms": 0
            })))
            .mount(&server)
            .await;

        // Mock query endpoint with mismatched LSN proof
        Mock::given(method("POST"))
            .and(path("/api/query"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "columns": ["status", "count"],
                "rows": [["paid", "2"]],
                "proof": "human wording may change",
                "proof_data": {
                    "target_lsn": 0xA000_0042_u64,
                    "visible_lsn": 0xA000_0043_u64
                }
            })))
            .mount(&server)
            .await;

        let adapter = GrayDbAdapter::new(server.uri());

        let invocation = QueryInvocation {
            id: QueryId::Q1,
            parameters: QueryParameters::for_checkpoint(
                20260901,
                LogicalCheckpoint {
                    sequence: 17,
                    source_lsn: 0xA000_0043,
                },
            ),
            checkpoint: LogicalCheckpoint {
                sequence: 17,
                source_lsn: 0xA000_0043,
            },
            target_lsn: 0xA000_0043,
        };

        let err = adapter.query(&invocation).await.unwrap_err();
        assert!(
            err.to_string().contains("LSN proof mismatch"),
            "error: {}",
            err
        );
    }

    #[test]
    fn format_lsn_roundtrip() {
        let lsn = 0xA000_0043;
        let formatted = format_lsn(lsn);
        let parsed = parse_lsn(&formatted).unwrap();
        assert_eq!(parsed, lsn);
    }
}

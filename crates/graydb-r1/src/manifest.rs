use crate::generator::cycle_ranges;
use crate::{CopyBatch, DeterministicGenerator, Event, EventSink, RunDirectory, Table};
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::SinkExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio_postgres::Client;

pub const PUBLISHED_TABLE_BYTES_SQL: &str = "SELECT sum(pg_table_size(c.oid))::bigint AS published_table_bytes\nFROM pg_class c\nJOIN pg_namespace n ON n.oid = c.relnamespace\nJOIN pg_publication_tables p\n  ON p.schemaname = n.nspname\n AND p.tablename = c.relname\nWHERE p.pubname = 'graydb_r1_pub'";

#[async_trait]
pub trait PublishedSizeProbe: Send {
    async fn published_size(&mut self) -> Result<u64>;
}

#[async_trait]
pub trait CopySink: Send {
    async fn copy(&mut self, batch: &CopyBatch) -> Result<()>;
}

/// PostgreSQL adapter used by the real service harness. Each generated batch
/// is sent through COPY, preserving the generator's exact tab-delimited bytes.
pub struct PostgresCopySink {
    client: Client,
}
impl PostgresCopySink {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}
#[async_trait]
impl CopySink for PostgresCopySink {
    async fn copy(&mut self, batch: &CopyBatch) -> Result<()> {
        let sql = format!(
            "COPY r1.{} FROM STDIN WITH (FORMAT text)",
            table_name(batch.table)
        );
        let sink = self
            .client
            .copy_in(&sql)
            .await
            .context("starting PostgreSQL COPY")?;
        futures_util::pin_mut!(sink);
        sink.send(Bytes::from(batch.bytes.clone()))
            .await
            .context("writing PostgreSQL COPY batch")?;
        sink.close()
            .await
            .context("finishing PostgreSQL COPY batch")?;
        Ok(())
    }
}

/// Performs ANALYZE before the frozen publication-size query, so loader
/// termination always uses a freshly measured PostgreSQL value.
pub struct PostgresPublishedSizeProbe {
    client: Client,
}
impl PostgresPublishedSizeProbe {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}
#[async_trait]
impl PublishedSizeProbe for PostgresPublishedSizeProbe {
    async fn published_size(&mut self) -> Result<u64> {
        self.client.batch_execute("ANALYZE r1.tenants; ANALYZE r1.customers; ANALYZE r1.orders; ANALYZE r1.order_events;").await.context("analyzing published tables")?;
        let row = self
            .client
            .query_one(PUBLISHED_TABLE_BYTES_SQL, &[])
            .await
            .context("measuring published table bytes")?;
        let bytes: Option<i64> = row.get(0);
        Ok(bytes
            .unwrap_or(0)
            .try_into()
            .context("published table size was negative")?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchManifest {
    pub table: Table,
    pub rows: u64,
    pub bytes: u64,
    pub sha256: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableManifest {
    pub rows: u64,
    pub table_bytes: u64,
    pub index_bytes: u64,
    pub total_relation_bytes: u64,
}

/// The stable, comparable portion of a dataset manifest. It intentionally has
/// no run identifier, host path, or wall-clock timing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetIdentity {
    pub seed: u64,
    pub schema_sha256: String,
    pub dictionary_sha256: String,
    pub published_table_bytes: u64,
    pub tables: BTreeMap<String, TableManifest>,
    pub batches: Vec<BatchManifest>,
    pub postgres_version: String,
    pub postgres_settings: BTreeMap<String, String>,
    pub initial_lsn: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetManifest {
    pub seed: u64,
    pub schema_sha256: String,
    pub dictionary_sha256: String,
    pub published_table_bytes: u64,
    pub tables: BTreeMap<String, TableManifest>,
    pub batches: Vec<BatchManifest>,
    pub postgres_version: String,
    pub postgres_settings: BTreeMap<String, String>,
    pub initial_lsn: Option<String>,
    pub load_started_unix_ms: u128,
    pub load_finished_unix_ms: u128,
    pub generation_ms: u128,
    pub copy_ms: u128,
    pub analyze_ms: u128,
}

impl DatasetManifest {
    pub fn identity(&self) -> DatasetIdentity {
        DatasetIdentity {
            seed: self.seed,
            schema_sha256: self.schema_sha256.clone(),
            dictionary_sha256: self.dictionary_sha256.clone(),
            published_table_bytes: self.published_table_bytes,
            tables: self.tables.clone(),
            batches: self.batches.clone(),
            postgres_version: self.postgres_version.clone(),
            postgres_settings: self.postgres_settings.clone(),
            initial_lsn: self.initial_lsn.clone(),
        }
    }
    pub fn content_hash(&self) -> Result<String> {
        let bytes = serde_json::to_vec(&self.identity()).context("serializing dataset identity")?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }

    /// Atomically publishes the manifest beneath Task 2's run directory.
    /// An empty placeholder created by RunDirectory is treated as unpublished.
    pub fn write_immutable(&self, run_root: impl AsRef<Path>) -> Result<()> {
        let root = run_root.as_ref();
        let final_path = root.join("dataset-manifest.json");
        if final_path.exists() && fs::metadata(&final_path)?.len() != 0 {
            let existing: DatasetManifest = serde_json::from_slice(&fs::read(&final_path)?)
                .context("reading existing dataset manifest")?;
            if existing.content_hash()? != self.content_hash()? {
                return Err(anyhow!(
                    "refusing to overwrite immutable dataset manifest with different content hash"
                ));
            }
            return Ok(());
        }
        let partial = root.join("dataset-manifest.json.partial");
        let bytes = serde_json::to_vec_pretty(self).context("serializing dataset manifest")?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&partial)
            .with_context(|| format!("opening {}", partial.display()))?;
        file.write_all(&bytes)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        drop(file);
        fs::rename(&partial, &final_path)?;
        fs::File::open(root)?.sync_all()?;
        Ok(())
    }

    pub fn publish_to_run(&self, run: &RunDirectory, events: &mut EventSink) -> Result<()> {
        self.write_immutable(run.root())?;
        events.emit(
            &Event::info("dataset_manifest", "published immutable dataset manifest")
                .with_field("content_hash", self.content_hash()?),
        )?;
        Ok(())
    }
}

pub struct DatasetLoader<P, S> {
    probe: P,
    sink: S,
    generator: DeterministicGenerator,
}
impl<P, S> DatasetLoader<P, S>
where
    P: PublishedSizeProbe,
    S: CopySink,
{
    pub fn with_probe(probe: P, sink: S, seed: u64) -> Self {
        Self {
            probe,
            sink,
            generator: DeterministicGenerator::new(seed),
        }
    }

    /// Loads full table-ratio cycles and only accepts the post-ANALYZE measured
    /// published size. Table metrics can be enriched by the PostgreSQL runner.
    pub async fn load_until(mut self, minimum_bytes: u64) -> Result<DatasetManifest> {
        let started = now_ms();
        let mut generation_ms = 0;
        let mut copy_ms = 0;
        let mut batches = Vec::new();
        let mut tables = BTreeMap::new();
        for cycle in 1.. {
            for range in cycle_ranges(cycle)
                .into_iter()
                .skip((cycle as usize - 1) * 4)
            {
                let generated_at = Instant::now();
                let generated = self.generator.copy_batches(range.table, range.range)?;
                generation_ms += generated_at.elapsed().as_millis();
                for batch in generated {
                    let copied_at = Instant::now();
                    self.sink.copy(&batch).await?;
                    copy_ms += copied_at.elapsed().as_millis();
                    let entry = tables.entry(table_name(batch.table).to_string()).or_insert(
                        TableManifest {
                            rows: 0,
                            table_bytes: 0,
                            index_bytes: 0,
                            total_relation_bytes: 0,
                        },
                    );
                    entry.rows += batch.rows;
                    batches.push(BatchManifest {
                        table: batch.table,
                        rows: batch.rows,
                        bytes: batch.bytes.len() as u64,
                        sha256: batch.sha256,
                    });
                }
            }
            let analyzed_at = Instant::now();
            // Concrete PostgreSQL sinks run ANALYZE before their probe; keeping it
            // out of this generic seam makes the measured loop service-testable.
            let published_table_bytes = self.probe.published_size().await?;
            let analyze_ms = analyzed_at.elapsed().as_millis();
            if published_table_bytes >= minimum_bytes {
                return Ok(DatasetManifest {
                    seed: self.generator.seed,
                    schema_sha256: String::new(),
                    dictionary_sha256: String::new(),
                    published_table_bytes,
                    tables,
                    batches,
                    postgres_version: String::new(),
                    postgres_settings: BTreeMap::new(),
                    initial_lsn: None,
                    load_started_unix_ms: started,
                    load_finished_unix_ms: now_ms(),
                    generation_ms,
                    copy_ms,
                    analyze_ms,
                });
            }
        }
        unreachable!()
    }
}

pub fn table_name(table: Table) -> &'static str {
    match table {
        Table::Tenants => "tenants",
        Table::Customers => "customers",
        Table::Orders => "orders",
        Table::OrderEvents => "order_events",
    }
}
fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before epoch")
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    struct FakeSizeProbe(VecDeque<u64>);
    impl FakeSizeProbe {
        fn new(values: impl IntoIterator<Item = u64>) -> Self {
            Self(values.into_iter().collect())
        }
    }
    #[async_trait]
    impl PublishedSizeProbe for FakeSizeProbe {
        async fn published_size(&mut self) -> Result<u64> {
            Ok(self.0.pop_front().unwrap())
        }
    }
    #[derive(Default)]
    struct FakeCopySink {
        batches: Vec<CopyBatch>,
    }
    #[async_trait]
    impl CopySink for FakeCopySink {
        async fn copy(&mut self, batch: &CopyBatch) -> Result<()> {
            self.batches.push(batch.clone());
            Ok(())
        }
    }
    #[tokio::test]
    async fn loader_stops_only_after_measured_threshold() {
        let loader = DatasetLoader::with_probe(
            FakeSizeProbe::new([400, 800, 1_200]),
            FakeCopySink::default(),
            100,
        );
        let manifest = loader.load_until(1_000).await.unwrap();
        assert_eq!(manifest.published_table_bytes, 1_200);
        assert_eq!(manifest.batches.len(), 12);
    }
    fn fixture_manifest() -> DatasetManifest {
        DatasetManifest {
            seed: 7,
            schema_sha256: "schema".into(),
            dictionary_sha256: "dict".into(),
            published_table_bytes: 1200,
            tables: BTreeMap::from([
                (
                    "tenants".into(),
                    TableManifest {
                        rows: 1,
                        table_bytes: 10,
                        index_bytes: 2,
                        total_relation_bytes: 12,
                    },
                ),
                (
                    "customers".into(),
                    TableManifest {
                        rows: 5,
                        table_bytes: 50,
                        index_bytes: 3,
                        total_relation_bytes: 53,
                    },
                ),
            ]),
            batches: vec![BatchManifest {
                table: Table::Tenants,
                rows: 1,
                bytes: 10,
                sha256: "a".into(),
            }],
            postgres_version: "16".into(),
            postgres_settings: BTreeMap::new(),
            initial_lsn: Some("0/1".into()),
            load_started_unix_ms: 1,
            load_finished_unix_ms: 2,
            generation_ms: 3,
            copy_ms: 4,
            analyze_ms: 5,
        }
    }
    #[test]
    fn content_hash_excludes_run_timestamps() {
        let a = fixture_manifest();
        let mut b = a.clone();
        b.load_started_unix_ms += 50_000;
        b.load_finished_unix_ms += 50_000;
        assert_eq!(a.content_hash().unwrap(), b.content_hash().unwrap());
    }
    #[test]
    fn immutable_write_refuses_different_content() {
        let dir = tempfile::tempdir().unwrap();
        let a = fixture_manifest();
        a.write_immutable(dir.path()).unwrap();
        a.write_immutable(dir.path()).unwrap();
        let mut b = a.clone();
        b.seed = 8;
        assert!(b.write_immutable(dir.path()).is_err());
    }
}

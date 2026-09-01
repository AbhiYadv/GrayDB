use crate::workload::TransactionPlan;
use crate::{Event, EventSink};
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, OpenOptions};
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerEntry {
    pub sequence: u64,
    pub xid: u32,
    pub source_lsn: u64,
    pub operation_sha256: String,
    pub committed_unix_ms: u128,
    pub previous_entry_sha256: String,
    pub entry_sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitState {
    Committed,
    UnknownCommit,
}

pub struct IntentLog {
    path: PathBuf,
    sync_path: PathBuf,
}

impl IntentLog {
    pub fn create(dir: impl AsRef<Path>) -> Result<Self> {
        fs::create_dir_all(dir.as_ref())?;
        Ok(Self {
            path: dir.as_ref().join("workload-intents.jsonl"),
            sync_path: dir.as_ref().join("sync_data"),
        })
    }

    pub fn append(&self, plan: &TransactionPlan) -> Result<()> {
        self.append_with_sink(plan, None)
    }

    pub fn append_with_sink(
        &self,
        plan: &TransactionPlan,
        mut sink: Option<&mut EventSink>,
    ) -> Result<()> {
        let line = serde_json::to_string(plan)?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        writeln!(file, "{line}")?;
        file.sync_data()?;
        let mut sync_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.sync_path)?;
        writeln!(sync_file, "{}", plan.sequence)?;
        sync_file.sync_data()?;
        if let Some(event_sink) = sink.as_deref_mut() {
            event_sink.emit(
                &Event::info("workload", "intent appended").with_field("sequence", plan.sequence),
            )?;
        }
        Ok(())
    }

    pub fn read_all(&self) -> Result<Vec<TransactionPlan>> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        std::io::BufReader::new(std::fs::File::open(&self.path)?)
            .lines()
            .map(|line| Ok(serde_json::from_str::<TransactionPlan>(&line?)?))
            .collect()
    }
}

pub struct CommittedLedger {
    dir: PathBuf,
    entries: Vec<LedgerEntry>,
}

impl CommittedLedger {
    pub fn create(dir: impl AsRef<Path>) -> Result<Self> {
        fs::create_dir_all(dir.as_ref())?;
        let path = dir.as_ref().join("workload-ledger.jsonl");
        OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            dir: dir.as_ref().to_path_buf(),
            entries: Vec::new(),
        })
    }

    pub fn append(&mut self, mut entry: LedgerEntry) -> Result<()> {
        let expected_previous = self
            .entries
            .last()
            .map(|existing| existing.entry_sha256.clone())
            .unwrap_or_default();
        if entry.sequence != self.next_sequence() {
            return Err(anyhow!(
                "ledger sequence {}, expected {}",
                entry.sequence,
                self.next_sequence()
            ));
        }
        if entry.previous_entry_sha256 != expected_previous {
            return Err(anyhow!("ledger previous hash mismatch"));
        }
        entry.entry_sha256 = entry_hash(&entry);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join("workload-ledger.jsonl"))?;
        writeln!(file, "{}", serde_json::to_string(&entry)?)?;
        file.sync_data()?;
        self.entries.push(entry);
        Ok(())
    }

    pub fn resume(dir: impl AsRef<Path>) -> Result<Self> {
        let mut ledger = Self {
            dir: dir.as_ref().to_path_buf(),
            entries: Vec::new(),
        };
        let path = dir.as_ref().join("workload-ledger.jsonl");
        if !path.exists() {
            return Ok(ledger);
        }
        for line in std::io::BufReader::new(std::fs::File::open(path)?).lines() {
            let entry: LedgerEntry = serde_json::from_str(&line?)?;
            if entry.sequence != ledger.next_sequence() {
                return Err(anyhow!("ledger gap or duplicate"));
            }
            if entry.entry_sha256 != entry_hash(&entry) {
                return Err(anyhow!("ledger checksum mismatch"));
            }
            if entry.previous_entry_sha256
                != ledger
                    .entries
                    .last()
                    .map(|existing| existing.entry_sha256.clone())
                    .unwrap_or_default()
            {
                return Err(anyhow!("ledger chain mismatch"));
            }
            ledger.entries.push(entry);
        }
        Ok(ledger)
    }

    pub fn classify(&self, plan: &TransactionPlan) -> CommitState {
        if self.entries.iter().any(|entry| {
            entry.sequence == plan.sequence && entry.operation_sha256 == plan.operation_sha256
        }) {
            CommitState::Committed
        } else {
            CommitState::UnknownCommit
        }
    }

    pub fn next_sequence(&self) -> u64 {
        self.entries
            .last()
            .map(|entry| entry.sequence + 1)
            .unwrap_or(1)
    }

    pub fn entries(&self) -> &[LedgerEntry] {
        &self.entries
    }
}

fn entry_hash(entry: &LedgerEntry) -> String {
    let mut copy = entry.clone();
    copy.entry_sha256.clear();
    let bytes = serde_json::to_vec(&copy).expect("entry serialization");
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WorkloadPlanner;

    fn fixture_entry(sequence: u64, previous: &str) -> LedgerEntry {
        LedgerEntry {
            sequence,
            xid: 9001,
            source_lsn: 100 + sequence,
            operation_sha256: format!("{:064x}", sequence),
            committed_unix_ms: sequence as u128,
            previous_entry_sha256: previous.to_string(),
            entry_sha256: String::new(),
        }
    }

    fn corrupt_second_line(path: &Path) {
        let mut bytes = fs::read(path).unwrap();
        let first_newline = bytes.iter().position(|byte| *byte == b'\n').unwrap();
        bytes[first_newline + 3] ^= 1;
        fs::write(path, bytes).unwrap();
    }

    #[test]
    fn resume_rejects_gap_duplicate_and_bad_checksum() {
        let dir = tempfile::tempdir().unwrap();
        let mut ledger = CommittedLedger::create(dir.path()).unwrap();
        ledger.append(fixture_entry(1, "")).unwrap();
        let previous = ledger.entries()[0].entry_sha256.clone();
        ledger.append(fixture_entry(2, &previous)).unwrap();
        assert_eq!(
            CommittedLedger::resume(dir.path()).unwrap().next_sequence(),
            3
        );

        let path = dir.path().join("workload-ledger.jsonl");
        corrupt_second_line(&path);
        assert!(CommittedLedger::resume(dir.path()).is_err());

        let duplicate_dir = tempfile::tempdir().unwrap();
        let mut duplicate = CommittedLedger::create(duplicate_dir.path()).unwrap();
        duplicate.append(fixture_entry(1, "")).unwrap();
        let previous = duplicate.entries()[0].entry_sha256.clone();
        duplicate.append(fixture_entry(2, &previous)).unwrap();
        assert!(duplicate.append(fixture_entry(2, &previous)).is_err());
    }

    #[test]
    fn intent_without_ledger_entry_is_unknown_commit() {
        let dir = tempfile::tempdir().unwrap();
        let planner = WorkloadPlanner::new(20260901);
        let intent_log = IntentLog::create(dir.path()).unwrap();
        let pending = planner.plan(1);
        intent_log.append(&pending).unwrap();
        let intents = intent_log.read_all().unwrap();
        assert_eq!(intents, vec![pending.clone()]);

        let mut ledger = CommittedLedger::create(dir.path()).unwrap();
        assert_eq!(ledger.classify(&pending), CommitState::UnknownCommit);

        let entry = fixture_entry(pending.sequence, "");
        ledger.append(entry).unwrap();
        assert_eq!(ledger.classify(&pending), CommitState::UnknownCommit);

        let committed = LedgerEntry {
            sequence: pending.sequence,
            xid: 9001,
            source_lsn: 101,
            operation_sha256: pending.operation_sha256.clone(),
            committed_unix_ms: 1,
            previous_entry_sha256: String::new(),
            entry_sha256: String::new(),
        };
        let committed_dir = tempfile::tempdir().unwrap();
        let mut committed_ledger = CommittedLedger::create(committed_dir.path()).unwrap();
        committed_ledger.append(committed).unwrap();
        assert_eq!(committed_ledger.classify(&pending), CommitState::Committed);
    }
}

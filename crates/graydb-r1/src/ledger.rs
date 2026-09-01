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
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        writeln!(f, "{line}")?;
        f.sync_data()?;
        let mut s = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.sync_path)?;
        writeln!(s, "{}", plan.sequence)?;
        s.sync_data()?;
        if let Some(es) = sink.as_deref_mut() {
            es.emit(
                &Event::info("workload", "intent appended").with_field("sequence", plan.sequence),
            )?;
        }
        Ok(())
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
    pub fn append(&mut self, mut e: LedgerEntry) -> Result<()> {
        let expected = self
            .entries
            .last()
            .map(|x| x.entry_sha256.clone())
            .unwrap_or_default();
        if e.sequence != self.next_sequence() {
            return Err(anyhow!(
                "ledger sequence {}, expected {}",
                e.sequence,
                self.next_sequence()
            ));
        }
        if e.previous_entry_sha256 != expected {
            return Err(anyhow!("ledger previous hash mismatch"));
        }
        e.entry_sha256 = entry_hash(&e);
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join("workload-ledger.jsonl"))?;
        writeln!(f, "{}", serde_json::to_string(&e)?)?;
        f.sync_data()?;
        self.entries.push(e);
        Ok(())
    }
    pub fn resume(dir: impl AsRef<Path>) -> Result<Self> {
        let mut me = Self {
            dir: dir.as_ref().to_path_buf(),
            entries: Vec::new(),
        };
        let path = dir.as_ref().join("workload-ledger.jsonl");
        if !path.exists() {
            return Ok(me);
        };
        for line in std::io::BufReader::new(std::fs::File::open(path)?).lines() {
            let e: LedgerEntry = serde_json::from_str(&line?)?;
            if e.sequence != me.next_sequence() {
                return Err(anyhow!("ledger gap or duplicate"));
            }
            if e.entry_sha256 != entry_hash(&e) {
                return Err(anyhow!("ledger checksum mismatch"));
            }
            if e.previous_entry_sha256
                != me
                    .entries
                    .last()
                    .map(|x| x.entry_sha256.clone())
                    .unwrap_or_default()
            {
                return Err(anyhow!("ledger chain mismatch"));
            }
            me.entries.push(e);
        }
        Ok(me)
    }
    pub fn next_sequence(&self) -> u64 {
        self.entries.last().map(|e| e.sequence + 1).unwrap_or(1)
    }
    pub fn entries(&self) -> &[LedgerEntry] {
        &self.entries
    }
}
fn entry_hash(e: &LedgerEntry) -> String {
    let mut c = e.clone();
    c.entry_sha256.clear();
    let b = serde_json::to_vec(&c).unwrap();
    let d = Sha256::digest(b);
    d.iter().map(|x| format!("{x:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    fn entry(sequence: u64, previous: &str) -> LedgerEntry {
        LedgerEntry {
            sequence,
            xid: 9001,
            source_lsn: 100 + sequence,
            operation_sha256: format!("op-{sequence}"),
            committed_unix_ms: sequence as u128,
            previous_entry_sha256: previous.into(),
            entry_sha256: String::new(),
        }
    }
    #[test]
    fn ledger_resume_validates_hash_chain() {
        let dir = tempfile::tempdir().unwrap();
        let mut ledger = CommittedLedger::create(dir.path()).unwrap();
        ledger.append(entry(1, "")).unwrap();
        let previous = ledger.entries()[0].entry_sha256.clone();
        ledger.append(entry(2, &previous)).unwrap();
        assert_eq!(
            CommittedLedger::resume(dir.path()).unwrap().next_sequence(),
            3
        );
        let path = dir.path().join("workload-ledger.jsonl");
        let mut bytes = fs::read(&path).unwrap();
        let first_newline = bytes.iter().position(|b| *b == b'\n').unwrap();
        bytes[first_newline + 3] ^= 1;
        fs::write(path, bytes).unwrap();
        assert!(CommittedLedger::resume(dir.path()).is_err());
    }
}

use crate::workload::TransactionPlan;
use crate::{Event, EventSink};
use anyhow::{anyhow, Context, Result};
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
    /// Byte offset of the verified prefix of `workload-ledger.jsonl`.  The
    /// file is append-only, so refresh() only parses and hash-verifies the
    /// tail beyond this offset.
    loaded_bytes: u64,
}

impl CommittedLedger {
    pub fn create(dir: impl AsRef<Path>) -> Result<Self> {
        fs::create_dir_all(dir.as_ref())?;
        let path = dir.as_ref().join("workload-ledger.jsonl");
        OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            dir: dir.as_ref().to_path_buf(),
            entries: Vec::new(),
            loaded_bytes: 0,
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
            loaded_bytes: 0,
        };
        ledger.refresh()?;
        Ok(ledger)
    }

    /// Incrementally loads and hash-verifies ledger lines appended since the
    /// last refresh.  A final line that is not yet newline-terminated (the
    /// writer is mid-append) is left for the next refresh; a corrupt line in
    /// the verified interior is a hard error.  ponytail: refresh is O(new
    /// tail); a full reload path exists via resume() if the append-only
    /// invariant is ever broken.
    pub fn refresh(&mut self) -> Result<()> {
        let path = self.dir.join("workload-ledger.jsonl");
        if !path.exists() {
            return Ok(());
        }
        let mut file = std::fs::File::open(&path)?;
        let file_len = file.metadata()?.len();
        if file_len < self.loaded_bytes {
            return Err(anyhow!(
                "ledger file shrank from {} to {} bytes; append-only invariant broken",
                self.loaded_bytes,
                file_len
            ));
        }
        if file_len == self.loaded_bytes {
            return Ok(());
        }
        use std::io::{BufRead, Seek, SeekFrom};
        file.seek(SeekFrom::Start(self.loaded_bytes))?;
        let mut reader = std::io::BufReader::new(file);
        let mut consumed = self.loaded_bytes;
        let mut line = String::new();
        loop {
            let line_start = consumed;
            line.clear();
            let read = reader.read_line(&mut line)?;
            if read == 0 {
                break;
            }
            consumed += read as u64;
            let trimmed = line.trim_end_matches(['\n', '\r']);
            if trimmed.is_empty() {
                continue;
            }
            let entry: LedgerEntry = match serde_json::from_str(trimmed) {
                Ok(entry) => entry,
                Err(error) if consumed >= file_len && !line.ends_with('\n') => {
                    // Unterminated final line: the writer has not finished
                    // this append.  Leave it for the next refresh.  A
                    // corrupt newline-terminated line stays a hard error.
                    let _ = error;
                    consumed = line_start;
                    break;
                }
                Err(error) => return Err(error).context("ledger line is corrupt"),
            };
            if entry.sequence != self.next_sequence() {
                return Err(anyhow!("ledger gap or duplicate"));
            }
            if entry.entry_sha256 != entry_hash(&entry) {
                return Err(anyhow!("ledger checksum mismatch"));
            }
            if entry.previous_entry_sha256
                != self
                    .entries
                    .last()
                    .map(|existing| existing.entry_sha256.clone())
                    .unwrap_or_default()
            {
                return Err(anyhow!("ledger chain mismatch"));
            }
            self.entries.push(entry);
        }
        self.loaded_bytes = consumed;
        Ok(())
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

    #[test]
    fn refresh_loads_only_the_appended_tail() {
        let dir = tempfile::tempdir().unwrap();
        let mut ledger = CommittedLedger::create(dir.path()).unwrap();
        ledger.append(fixture_entry(1, "")).unwrap();
        let previous = ledger.entries()[0].entry_sha256.clone();
        ledger.append(fixture_entry(2, &previous)).unwrap();

        // A long-running consumer caches the ledger at two entries while the
        // writer appends more.
        let mut cached = CommittedLedger::resume(dir.path()).unwrap();
        assert_eq!(cached.entries().len(), 2);

        let mut writer = CommittedLedger::resume(dir.path()).unwrap();
        let tip = writer.entries()[1].entry_sha256.clone();
        writer.append(fixture_entry(3, &tip)).unwrap();

        cached.refresh().unwrap();
        assert_eq!(cached.entries().len(), 3);
        assert_eq!(cached.next_sequence(), 4);
        // No new lines: refresh is a no-op.
        cached.refresh().unwrap();
        assert_eq!(cached.entries().len(), 3);
    }

    #[test]
    fn refresh_leaves_a_partial_final_line_for_the_next_refresh() {
        let dir = tempfile::tempdir().unwrap();
        let mut ledger = CommittedLedger::create(dir.path()).unwrap();
        ledger.append(fixture_entry(1, "")).unwrap();
        let mut cached = CommittedLedger::resume(dir.path()).unwrap();

        let writer = CommittedLedger::resume(dir.path()).unwrap();
        let tip = writer.entries()[0].entry_sha256.clone();
        let mut entry = fixture_entry(2, &tip);
        entry.entry_sha256 = entry_hash(&entry);
        let line = serde_json::to_string(&entry).unwrap();

        // Writer is mid-append: half the JSON, no newline yet.
        let path = dir.path().join("workload-ledger.jsonl");
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        file.write_all(line[..line.len() / 2].as_bytes()).unwrap();
        cached.refresh().unwrap();
        assert_eq!(cached.entries().len(), 1);

        // The writer finishes the append.
        file.write_all(line[line.len() / 2..].as_bytes()).unwrap();
        file.write_all(b"\n").unwrap();
        file.sync_data().unwrap();
        cached.refresh().unwrap();
        assert_eq!(cached.entries().len(), 2);
        assert_eq!(cached.next_sequence(), 3);
    }

    #[test]
    fn refresh_rejects_a_shrinking_ledger_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut ledger = CommittedLedger::create(dir.path()).unwrap();
        ledger.append(fixture_entry(1, "")).unwrap();
        let mut cached = CommittedLedger::resume(dir.path()).unwrap();
        std::fs::write(dir.path().join("workload-ledger.jsonl"), b"").unwrap();
        assert!(cached.refresh().is_err());
    }
}

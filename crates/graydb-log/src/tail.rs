//! LogTail: incremental reader over a live frame log (the charter's `tail(from_lsn)`).
//! Remembers a (segment, byte offset) cursor; each `read_new` returns only complete
//! frames appended since the last call. A torn frame at the tail of the LAST segment
//! is "still being written" — the cursor stops before it and retries next call.
//!
//! Truncation awareness: `FrameLog::resume` (crash/kill recovery) may SHRINK the last
//! segment back to the durable boundary. A shrink can only remove frames past the
//! last transaction-complete frame (resume's invariant), so the tail reports
//! `rewound = true` and the consumer must abort its open (uncommitted) transaction —
//! those frames will be re-delivered by the fresh replication session.

use crate::{decode_frame, Frame};
use anyhow::{bail, Context, Result};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct LogTail {
    dir: PathBuf,
    /// Name (not path) of the segment the cursor is in; None = not started.
    current: Option<String>,
    offset: u64,
}

#[derive(Debug, Default)]
pub struct TailBatch {
    pub frames: Vec<Frame>,
    /// The log shrank beneath the cursor: uncommitted tail frames were truncated by
    /// a resume. Consumer must abort its open transaction before applying `frames`.
    pub rewound: bool,
}

impl LogTail {
    pub fn new(dir: &Path) -> Self {
        LogTail {
            dir: dir.to_path_buf(),
            current: None,
            offset: 0,
        }
    }

    fn segment_names(&self) -> Result<Vec<String>> {
        let mut names: Vec<String> = match std::fs::read_dir(&self.dir) {
            Ok(rd) => rd
                .filter_map(|e| e.ok())
                .filter_map(|e| e.file_name().into_string().ok())
                .filter(|n| n.ends_with(".gdl"))
                .collect(),
            Err(_) => Vec::new(), // dir not created yet: nothing to read
        };
        names.sort();
        Ok(names)
    }

    /// Read all complete frames appended since the last call.
    pub fn read_new(&mut self) -> Result<TailBatch> {
        let mut out = TailBatch::default();
        let segs = self.segment_names()?;
        if segs.is_empty() {
            return Ok(out);
        }
        // Establish or re-validate the cursor segment.
        let mut idx = match &self.current {
            None => {
                self.current = Some(segs[0].clone());
                self.offset = 0;
                0
            }
            Some(name) => match segs.iter().position(|s| s == name) {
                Some(i) => i,
                None => {
                    // Our segment was deleted (resume truncated an earlier boundary
                    // and removed later segments). Everything we had past the durable
                    // boundary is gone; restart from the last segment that still
                    // exists at its END (its content up to EOF is durable-or-earlier
                    // than what we already consumed — resume only ever cuts back to
                    // a point we have already read, since applies lag durability).
                    out.rewound = true;
                    let last = segs.len() - 1;
                    self.current = Some(segs[last].clone());
                    self.offset = std::fs::metadata(self.dir.join(&segs[last]))
                        .map(|m| m.len())
                        .unwrap_or(0);
                    last
                }
            },
        };

        loop {
            let name = segs[idx].clone();
            let path = self.dir.join(&name);
            let len = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            if len < self.offset {
                // Shrunk beneath us: truncation removed uncommitted tail frames.
                out.rewound = true;
                self.offset = len;
            }
            if len > self.offset {
                let mut file = std::fs::File::open(&path)
                    .with_context(|| format!("opening {}", path.display()))?;
                file.seek(SeekFrom::Start(self.offset))?;
                let mut buf = Vec::with_capacity((len - self.offset) as usize);
                file.take(len - self.offset).read_to_end(&mut buf)?;
                let mut cursor = &buf[..];
                loop {
                    let before = cursor.len();
                    match decode_frame(&mut cursor) {
                        Ok(Some(frame)) => {
                            self.offset += (before - cursor.len()) as u64;
                            out.frames.push(frame);
                        }
                        Ok(None) => break,
                        Err(_) if idx + 1 == segs.len() => break, // torn tail: retry later
                        Err(e) => {
                            bail!("corrupt frame mid-log in {} (not at tail): {e}", name)
                        }
                    }
                }
            }
            // Advance to the next segment only once this one is fully consumed and a
            // successor exists (segments roll only after a synced commit).
            if idx + 1 < segs.len() && self.offset >= len {
                idx += 1;
                self.current = Some(segs[idx].clone());
                self.offset = 0;
            } else {
                break;
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tail_tests {
    use super::*;
    use crate::FrameLog;
    use bytes::Bytes;

    #[tokio::test]
    async fn tail_reads_incrementally_and_survives_resume_truncation() {
        let dir = std::env::temp_dir().join(format!("gdb-tail-{}", std::process::id()));
        let mut log = FrameLog::create(&dir, 1 << 20).await.unwrap();
        let mut tail = LogTail::new(&dir);

        log.append(1, 2, false, Bytes::from_static(b"B1"))
            .await
            .unwrap();
        log.append(2, 3, true, Bytes::from_static(b"C1"))
            .await
            .unwrap();
        let b = tail.read_new().unwrap();
        assert_eq!(b.frames.len(), 2);
        assert!(!b.rewound);

        // Uncommitted tail, then crash + resume (truncates it away).
        log.append(4, 5, false, Bytes::from_static(b"B2"))
            .await
            .unwrap();
        let b = tail.read_new().unwrap();
        assert_eq!(b.frames.len(), 1, "tail sees the uncommitted frame");
        drop(log);
        let mut log = FrameLog::resume(&dir, 1 << 20).await.unwrap();
        // Fresh session re-delivers the transaction, then commits it.
        log.append(4, 5, false, Bytes::from_static(b"B2"))
            .await
            .unwrap();
        log.append(5, 6, true, Bytes::from_static(b"C2"))
            .await
            .unwrap();

        let b = tail.read_new().unwrap();
        assert!(b.rewound, "truncation must be reported");
        assert_eq!(b.frames.len(), 2, "re-delivered txn readable after rewind");
        assert!(b.frames.iter().any(|f| f.txn_complete));

        // Nothing new -> empty, no rewind.
        let b = tail.read_new().unwrap();
        assert!(b.frames.is_empty() && !b.rewound);
        tokio::fs::remove_dir_all(&dir).await.ok();
    }
}

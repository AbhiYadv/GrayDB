//! graydb-log: the durable LSN-ordered frame log (the spine, I2).
//! Frame = { seq, lsn_start, lsn_end, txn_complete: bool, crc32c, raw pgoutput bytes }.
//! Exposes: append -> DurableMark watch, verify(range). The ack invariant lives at
//! this boundary: the durable mark advances ONLY past a transaction-complete,
//! checksummed, fsync'd frame prefix — and slot acknowledgment follows the mark,
//! never the socket.
//!
//! Rung 3 of the WAL ladder (Amendment A A3) also lives here: `set_stalled(true)`
//! degrades the write path — frames divert to a staging spill (never truth, never
//! acked); `set_stalled(false)` drains staging back into the durable segment in
//! order and only then lets the mark advance.
//!
//! Storage note (D-009): direct file IO + fsync, laid out one-segment-per-object so
//! the object_store backend can slot in for S3/MinIO later. The spike needs REAL
//! fsync custody for the invariant; object_store's local backend doesn't promise it.

pub mod tail;

use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;
use tokio::sync::watch;

pub const FRAME_MAGIC: u32 = 0x4744_4246; // "GDBF"
const HEADER_LEN: usize = 4 + 8 + 8 + 8 + 1 + 4; // magic..payload_len
const FLAG_TXN_COMPLETE: u8 = 0b0000_0001;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DurableMark {
    /// Sequence of the last durably-synced transaction-complete frame.
    pub seq: u64,
    /// Its lsn_end — the highest LSN the slot may be acknowledged to.
    pub lsn: u64,
    /// True once any commit frame is durable (distinguishes lsn=0 "nothing yet").
    pub valid: bool,
}

#[derive(Debug, Clone)]
pub struct Frame {
    pub seq: u64,
    pub lsn_start: u64,
    pub lsn_end: u64,
    pub txn_complete: bool,
    pub payload: Bytes,
}

pub fn encode_frame(f: &Frame) -> Bytes {
    let mut buf = BytesMut::with_capacity(HEADER_LEN + f.payload.len() + 4);
    buf.put_u32(FRAME_MAGIC);
    buf.put_u64(f.seq);
    buf.put_u64(f.lsn_start);
    buf.put_u64(f.lsn_end);
    buf.put_u8(if f.txn_complete { FLAG_TXN_COMPLETE } else { 0 });
    buf.put_u32(f.payload.len() as u32);
    buf.put_slice(&f.payload);
    let crc = crc32c::crc32c(&buf);
    buf.put_u32(crc);
    buf.freeze()
}

/// Decode one frame from the head of `data`, advancing it. Ok(None) = clean end.
pub fn decode_frame(data: &mut &[u8]) -> Result<Option<Frame>> {
    if data.is_empty() {
        return Ok(None);
    }
    if data.len() < HEADER_LEN + 4 {
        bail!("truncated frame header ({} trailing bytes)", data.len());
    }
    let full = *data;
    let mut hdr = *data;
    let magic = hdr.get_u32();
    if magic != FRAME_MAGIC {
        bail!("bad frame magic {magic:#010x}");
    }
    let seq = hdr.get_u64();
    let lsn_start = hdr.get_u64();
    let lsn_end = hdr.get_u64();
    let flags = hdr.get_u8();
    let payload_len = hdr.get_u32() as usize;
    let total = HEADER_LEN + payload_len + 4;
    if full.len() < total {
        bail!("truncated frame payload (seq {seq}: need {total}, have {})", full.len());
    }
    let crc_stored = (&full[HEADER_LEN + payload_len..]).get_u32();
    let crc_actual = crc32c::crc32c(&full[..HEADER_LEN + payload_len]);
    if crc_stored != crc_actual {
        bail!("crc mismatch on frame seq {seq}: stored {crc_stored:#x} actual {crc_actual:#x}");
    }
    let payload = Bytes::copy_from_slice(&full[HEADER_LEN..HEADER_LEN + payload_len]);
    *data = &full[total..];
    Ok(Some(Frame {
        seq,
        lsn_start,
        lsn_end,
        txn_complete: flags & FLAG_TXN_COMPLETE != 0,
        payload,
    }))
}

/// Durable LSN-ordered frame log over segment files: seg-{first_seq:016x}.gdl.
pub struct FrameLog {
    dir: PathBuf,
    staging_path: PathBuf,
    segment_max_bytes: u64,
    file: tokio::fs::File,
    segment_len: u64,
    next_seq: u64,
    /// Appended-but-not-yet-synced highest commit frame (seq, lsn_end).
    pending_commit: Option<(u64, u64)>,
    durable_tx: watch::Sender<DurableMark>,
    stalled: bool,
    /// Rung-3 staging: encoded frames diverted while stalled (mirrored to staging file).
    spill: Vec<Bytes>,
    spill_file: Option<tokio::fs::File>,
    pub spilled_frames: u64,
    pub total_frames: u64,
    pub total_commits: u64,
}

impl FrameLog {
    /// Open a fresh log in `dir` (demo-grade: starts at seq 0; recovery/resume from
    /// existing segments is SP7 territory and must replay-verify before continuing).
    pub async fn create(dir: &Path, segment_max_bytes: u64) -> Result<Self> {
        if dir.exists() {
            tokio::fs::remove_dir_all(dir).await.ok();
        }
        tokio::fs::create_dir_all(dir).await?;
        let first = dir.join(format!("seg-{:016x}.gdl", 0));
        let file = tokio::fs::File::create(&first).await?;
        let (durable_tx, _) = watch::channel(DurableMark::default());
        Ok(FrameLog {
            dir: dir.to_path_buf(),
            staging_path: dir.join("staging.spill"),
            segment_max_bytes,
            file,
            segment_len: 0,
            next_seq: 0,
            pending_commit: None,
            durable_tx,
            stalled: false,
            spill: Vec::new(),
            spill_file: None,
            spilled_frames: 0,
            total_frames: 0,
            total_commits: 0,
        })
    }

    /// Resume an EXISTING log after a crash (Demo 4/7, SP7). The recovery rule is the
    /// ack invariant read backwards: the durable prefix ends at the last
    /// transaction-complete frame; everything after it (unsynced, possibly torn) is
    /// TRUNCATED away — a restart never trusts a dying session's tail, it re-fetches
    /// from the source via a fresh replication session starting at the durable mark.
    pub async fn resume(dir: &Path, segment_max_bytes: u64) -> Result<Self> {
        anyhow::ensure!(dir.exists(), "no log to resume at {}", dir.display());
        let mut segs: Vec<PathBuf> = std::fs::read_dir(dir)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().map(|x| x == "gdl").unwrap_or(false))
            .collect();
        segs.sort();

        // Find the durable boundary: last txn-complete frame across all segments.
        let mut boundary: Option<(usize, u64, u64, u64)> = None; // (seg idx, end offset, seq, lsn)
        let mut frames = 0u64;
        let mut commits = 0u64;
        for (i, seg) in segs.iter().enumerate() {
            let data = std::fs::read(seg)?;
            let mut cursor = &data[..];
            let mut offset = 0u64;
            loop {
                let before = cursor.len();
                match decode_frame(&mut cursor) {
                    Ok(Some(frame)) => {
                        let flen = (before - cursor.len()) as u64;
                        offset += flen;
                        frames += 1;
                        if frame.txn_complete {
                            commits += 1;
                            boundary = Some((i, offset, frame.seq, frame.lsn_end));
                        }
                    }
                    Ok(None) => break,
                    // Torn tail: expected after a crash; the truncation below removes it.
                    Err(_) => break,
                }
            }
        }

        // Truncate past the boundary; drop later segments entirely.
        let (next_seq, mark) = match boundary {
            Some((seg_idx, end_offset, seq, lsn)) => {
                let f = std::fs::OpenOptions::new().write(true).open(&segs[seg_idx])?;
                f.set_len(end_offset)?;
                f.sync_all()?;
                for seg in &segs[seg_idx + 1..] {
                    std::fs::remove_file(seg)?;
                }
                (
                    seq + 1,
                    DurableMark {
                        seq,
                        lsn,
                        valid: true,
                    },
                )
            }
            None => {
                for seg in &segs {
                    std::fs::remove_file(seg)?;
                }
                (0, DurableMark::default())
            }
        };
        let staging = dir.join("staging.spill");
        std::fs::remove_file(&staging).ok(); // staging is never truth (A3 rung 3)

        let new_seg = dir.join(format!("seg-{next_seq:016x}.gdl"));
        let file = tokio::fs::File::create(&new_seg).await?;
        let (durable_tx, _) = watch::channel(mark);
        tracing::info!(
            dir = %dir.display(),
            durable_seq = mark.seq,
            durable_lsn = mark.lsn,
            frames,
            commits,
            "frame log resumed at durable boundary (tail truncated)"
        );
        Ok(FrameLog {
            dir: dir.to_path_buf(),
            staging_path: staging,
            segment_max_bytes,
            file,
            segment_len: 0,
            next_seq,
            pending_commit: None,
            durable_tx,
            stalled: false,
            spill: Vec::new(),
            spill_file: None,
            spilled_frames: 0,
            total_frames: frames,
            total_commits: commits,
        })
    }

    pub fn durable(&self) -> watch::Receiver<DurableMark> {
        self.durable_tx.subscribe()
    }

    pub fn durable_now(&self) -> DurableMark {
        *self.durable_tx.borrow()
    }

    pub fn is_stalled(&self) -> bool {
        self.stalled
    }

    /// Append one raw pgoutput message as a frame. Returns the assigned seq.
    /// When txn_complete and healthy, the write path syncs and the durable mark
    /// advances (ack may then — and only then — follow).
    pub async fn append(
        &mut self,
        lsn_start: u64,
        lsn_end: u64,
        txn_complete: bool,
        payload: Bytes,
    ) -> Result<u64> {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.total_frames += 1;
        if txn_complete {
            self.total_commits += 1;
        }
        let encoded = encode_frame(&Frame {
            seq,
            lsn_start,
            lsn_end,
            txn_complete,
            payload,
        });

        if self.stalled {
            // Rung 3: write path degraded — divert to staging (unsynced, unacked).
            if self.spill_file.is_none() {
                self.spill_file =
                    Some(tokio::fs::File::create(&self.staging_path).await?);
            }
            self.spill_file
                .as_mut()
                .expect("spill file just ensured")
                .write_all(&encoded)
                .await?;
            self.spill.push(encoded);
            self.spilled_frames += 1;
            return Ok(seq);
        }

        self.write_durable(seq, lsn_end, txn_complete, &encoded).await?;
        Ok(seq)
    }

    async fn write_durable(
        &mut self,
        seq: u64,
        lsn_end: u64,
        txn_complete: bool,
        encoded: &[u8],
    ) -> Result<()> {
        self.file.write_all(encoded).await?;
        self.segment_len += encoded.len() as u64;
        if txn_complete {
            self.pending_commit = Some((seq, lsn_end));
            // Sync on every commit frame: the honest (if unbatched) reading of the
            // ack invariant. Batching is an optimization for later, never a default.
            self.file.sync_all().await.context("fsync frame segment")?;
            let (dseq, dlsn) = self.pending_commit.take().expect("just set");
            self.durable_tx.send_replace(DurableMark {
                seq: dseq,
                lsn: dlsn,
                valid: true,
            });
            if self.segment_len >= self.segment_max_bytes {
                self.roll_segment().await?;
            }
        }
        Ok(())
    }

    async fn roll_segment(&mut self) -> Result<()> {
        let path = self.dir.join(format!("seg-{:016x}.gdl", self.next_seq));
        self.file = tokio::fs::File::create(&path).await?;
        self.segment_len = 0;
        tracing::info!(segment = %path.display(), "rolled frame-log segment");
        Ok(())
    }

    /// Enter/leave rung 3. Leaving drains staging into the durable segment IN ORDER,
    /// syncs, and only then lets the durable mark advance past the spilled commits.
    pub async fn set_stalled(&mut self, stalled: bool) -> Result<()> {
        if stalled == self.stalled {
            return Ok(());
        }
        if stalled {
            self.stalled = true;
            tracing::warn!("frame-log write path STALLED — rung 3 staging active");
            return Ok(());
        }
        // Resume: drain.
        let frames = std::mem::take(&mut self.spill);
        let mut last_commit: Option<(u64, u64)> = None;
        for encoded in &frames {
            self.file.write_all(encoded).await?;
            self.segment_len += encoded.len() as u64;
            // Recover (seq, lsn_end, flag) from the encoded header for mark tracking.
            let mut view = &encoded[..];
            view.advance(4);
            let seq = view.get_u64();
            let _lsn_start = view.get_u64();
            let lsn_end = view.get_u64();
            let flags = view.get_u8();
            if flags & FLAG_TXN_COMPLETE != 0 {
                last_commit = Some((seq, lsn_end));
            }
        }
        self.file.sync_all().await.context("fsync drained staging")?;
        if let Some((seq, lsn)) = last_commit {
            self.durable_tx.send_replace(DurableMark {
                seq,
                lsn,
                valid: true,
            });
        }
        self.spill_file = None;
        tokio::fs::remove_file(&self.staging_path).await.ok();
        self.stalled = false;
        tracing::info!(
            drained = frames.len(),
            "frame-log resumed — staging drained, durable mark advanced"
        );
        if self.segment_len >= self.segment_max_bytes {
            self.roll_segment().await?;
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct LogVerification {
    pub frames: u64,
    pub commits: u64,
    pub first_seq: Option<u64>,
    pub last_seq: Option<u64>,
    pub max_lsn_end: u64,
    pub seq_contiguous: bool,
    /// Commit-order monotonicity: end_lsn non-decreasing across txn-complete frames.
    /// (Interior record LSNs legitimately interleave across concurrent transactions —
    /// pgoutput delivers in COMMIT order, so only commit boundaries are ordered.)
    pub lsn_monotone: bool,
    /// A partially-written frame was found at the tail of the LAST segment.
    /// Expected on a live log (non-commit frames are unsynced); a defect after
    /// clean shutdown. Only tolerated when `allow_torn_tail` is set.
    pub torn_tail: bool,
}

/// Read every segment in order, verifying magic + crc on every frame and
/// seq contiguity / lsn_end monotonicity across the whole log.
/// Also invokes `inspect` per frame (payload custody checks live in callers).
/// `allow_torn_tail`: tolerate ONE truncated frame at the very end of the last
/// segment (live-log reads); any earlier decode error is always fatal.
pub fn verify_log(
    dir: &Path,
    allow_torn_tail: bool,
    mut inspect: impl FnMut(&Frame),
) -> Result<LogVerification> {
    let mut segs: Vec<PathBuf> = std::fs::read_dir(dir)
        .with_context(|| format!("reading log dir {}", dir.display()))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().map(|x| x == "gdl").unwrap_or(false))
        .collect();
    segs.sort();
    let nsegs = segs.len();

    let mut v = LogVerification {
        seq_contiguous: true,
        lsn_monotone: true,
        ..Default::default()
    };
    let mut expected_seq: Option<u64> = None;
    let mut last_commit_end = 0u64;
    for (i, seg) in segs.iter().enumerate() {
        let data = std::fs::read(seg)?;
        let mut cursor = &data[..];
        loop {
            let frame = match decode_frame(&mut cursor) {
                Ok(Some(f)) => f,
                Ok(None) => break,
                Err(e) => {
                    let is_last_segment = i + 1 == nsegs;
                    let truncated = e.to_string().contains("truncated");
                    if allow_torn_tail && is_last_segment && truncated {
                        v.torn_tail = true;
                        break;
                    }
                    return Err(e).with_context(|| format!("in {}", seg.display()));
                }
            };
            v.frames += 1;
            if frame.txn_complete {
                v.commits += 1;
            }
            if v.first_seq.is_none() {
                v.first_seq = Some(frame.seq);
            }
            if let Some(exp) = expected_seq {
                if frame.seq != exp {
                    v.seq_contiguous = false;
                }
            }
            expected_seq = Some(frame.seq + 1);
            if frame.txn_complete {
                if frame.lsn_end < last_commit_end {
                    v.lsn_monotone = false;
                }
                last_commit_end = frame.lsn_end;
            }
            v.max_lsn_end = v.max_lsn_end.max(frame.lsn_end);
            v.last_seq = Some(frame.seq);
            inspect(&frame);
        }
    }
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_roundtrip() {
        let f = Frame {
            seq: 7,
            lsn_start: 100,
            lsn_end: 200,
            txn_complete: true,
            payload: Bytes::from_static(b"C-fake-commit"),
        };
        let enc = encode_frame(&f);
        let mut view = &enc[..];
        let out = decode_frame(&mut view).unwrap().unwrap();
        assert!(view.is_empty());
        assert_eq!(out.seq, 7);
        assert_eq!(out.lsn_start, 100);
        assert_eq!(out.lsn_end, 200);
        assert!(out.txn_complete);
        assert_eq!(&out.payload[..], b"C-fake-commit");
    }

    #[test]
    fn corrupted_frame_fails_crc() {
        let f = Frame {
            seq: 1,
            lsn_start: 1,
            lsn_end: 2,
            txn_complete: false,
            payload: Bytes::from_static(b"payload"),
        };
        let enc = encode_frame(&f);
        let mut bad = enc.to_vec();
        let idx = HEADER_LEN + 2;
        bad[idx] ^= 0xFF;
        let mut view = &bad[..];
        assert!(decode_frame(&mut view).is_err());
    }

    #[test]
    fn truncated_tail_fails_loudly() {
        let f = Frame {
            seq: 1,
            lsn_start: 1,
            lsn_end: 2,
            txn_complete: false,
            payload: Bytes::from_static(b"payload"),
        };
        let enc = encode_frame(&f);
        let mut view = &enc[..enc.len() - 3];
        assert!(decode_frame(&mut view).is_err());
    }

    #[tokio::test]
    async fn durable_mark_only_advances_on_commit_frames() {
        let dir = std::env::temp_dir().join(format!("gdb-log-test-{}", std::process::id()));
        let mut log = FrameLog::create(&dir, 1 << 20).await.unwrap();
        assert!(!log.durable_now().valid);
        log.append(10, 20, false, Bytes::from_static(b"B")).await.unwrap();
        log.append(20, 30, false, Bytes::from_static(b"I")).await.unwrap();
        assert!(!log.durable_now().valid, "no commit yet => no durable mark");
        log.append(30, 40, true, Bytes::from_static(b"C")).await.unwrap();
        let m = log.durable_now();
        assert!(m.valid);
        assert_eq!(m.lsn, 40);
        assert_eq!(m.seq, 2);
        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    #[tokio::test]
    async fn resume_truncates_to_durable_boundary_and_continues_seq() {
        let dir = std::env::temp_dir().join(format!("gdb-log-resume-{}", std::process::id()));
        {
            let mut log = FrameLog::create(&dir, 1 << 20).await.unwrap();
            log.append(1, 2, false, Bytes::from_static(b"B1")).await.unwrap();
            log.append(2, 3, true, Bytes::from_static(b"C1")).await.unwrap(); // durable
            log.append(4, 5, false, Bytes::from_static(b"B2")).await.unwrap(); // unsynced tail
            // crash: drop without commit of txn 2
        }
        // Simulate a torn write at the very end.
        let seg = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.path()))
            .find(|p| p.extension().map(|x| x == "gdl").unwrap_or(false))
            .unwrap();
        let len = std::fs::metadata(&seg).unwrap().len();
        let f = std::fs::OpenOptions::new().write(true).open(&seg).unwrap();
        f.set_len(len - 1).unwrap(); // tear the trailing frame

        let mut log = FrameLog::resume(&dir, 1 << 20).await.unwrap();
        let m = log.durable_now();
        assert!(m.valid);
        assert_eq!((m.seq, m.lsn), (1, 3), "durable boundary = last commit frame");
        // Continue appending: seq must be contiguous after the boundary.
        log.append(6, 7, true, Bytes::from_static(b"C2")).await.unwrap();
        let v = verify_log(&dir, false, |_| {}).unwrap();
        assert_eq!(v.frames, 3, "torn tail gone, new frame appended");
        assert!(v.seq_contiguous);
        assert_eq!(v.commits, 2);
        assert_eq!(log.durable_now().lsn, 7);
        tokio::fs::remove_dir_all(&dir).await.ok();
    }

    #[tokio::test]
    async fn stall_diverts_and_resume_drains_in_order() {
        let dir = std::env::temp_dir().join(format!("gdb-log-stall-{}", std::process::id()));
        let mut log = FrameLog::create(&dir, 1 << 20).await.unwrap();
        log.append(1, 2, true, Bytes::from_static(b"C1")).await.unwrap();
        let before = log.durable_now();
        log.set_stalled(true).await.unwrap();
        log.append(3, 4, false, Bytes::from_static(b"I2")).await.unwrap();
        log.append(4, 5, true, Bytes::from_static(b"C2")).await.unwrap();
        assert_eq!(log.durable_now(), before, "stalled: mark must NOT advance");
        assert_eq!(log.spilled_frames, 2);
        log.set_stalled(false).await.unwrap();
        let after = log.durable_now();
        assert_eq!(after.lsn, 5);
        assert_eq!(after.seq, 2);
        let v = verify_log(&dir, false, |_| {}).unwrap();
        assert_eq!(v.frames, 3);
        assert_eq!(v.commits, 2);
        assert!(v.seq_contiguous && v.lsn_monotone);
        tokio::fs::remove_dir_all(&dir).await.ok();
    }
}

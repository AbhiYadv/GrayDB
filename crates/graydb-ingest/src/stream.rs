//! The SP2 ingest pump: CopyBoth stream -> graydb-log frames -> ack.
//! LAW (Amendment A, enforced here): the Standby Status Update NEVER reports
//! anything but graydb-log's durable mark — a transaction-complete, checksummed,
//! fsync'd prefix. Receipt is not custody; only the mark is.

use crate::repl::{ReplClient, ReplMsg};
use anyhow::Result;
use graydb_log::FrameLog;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PumpCommand {
    /// Rung 3: divert frames to staging, freeze the durable mark (and thus acks).
    pub stalled: bool,
    pub shutdown: bool,
}

#[derive(Debug, Default)]
pub struct IngestMetrics {
    pub frames: AtomicU64,
    pub commits: AtomicU64,
    pub spilled_frames: AtomicU64,
    pub durable_lsn: AtomicU64,
    pub acked_lsn: AtomicU64,
    /// Highest source WAL position this session has SEEN (keepalive wal_end or frame
    /// end). Not a durability claim — it is the "we have caught up to here" signal
    /// that makes `strong` (source-barrier) reads terminable: once stream_lsn >= B,
    /// every transaction that committed at or before B has been received, even though
    /// B itself may sit past the last commit (checkpoints etc. move the WAL head).
    pub stream_lsn: AtomicU64,
}

/// pgoutput v1 Commit message: 'C', u8 flags, u64 commit_lsn, u64 end_lsn, u64 ts.
/// end_lsn already points one past the transaction — the exact ack boundary.
fn commit_end_lsn(payload: &[u8]) -> Option<u64> {
    if payload.len() >= 26 && payload[0] == b'C' {
        Some(u64::from_be_bytes(payload[10..18].try_into().ok()?))
    } else {
        None
    }
}

/// Drive the replication stream into the frame log until shutdown.
/// Owns both the socket and the log; observers watch `metrics`.
pub async fn run_pump(
    mut repl: ReplClient,
    mut log: FrameLog,
    start_lsn: u64,
    mut ctrl: watch::Receiver<PumpCommand>,
    metrics: Arc<IngestMetrics>,
) -> Result<()> {
    // 1s cadence: keeps the slot ack fresh AND asks the source for a keepalive
    // (reply-requested status updates make the server answer with its wal_end,
    // which is what `strong` reads wait on).
    let mut status_interval = tokio::time::interval(Duration::from_secs(1));
    status_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            changed = ctrl.changed() => {
                if changed.is_err() {
                    break; // controller dropped: shut down
                }
                let cmd = *ctrl.borrow();
                if cmd.shutdown {
                    break;
                }
                log.set_stalled(cmd.stalled).await?;
                if !cmd.stalled {
                    // Resumed: staging drained durably — ack the recovered mark now.
                    let m = log.durable_now();
                    if m.valid {
                        repl.send_standby_status(m.lsn, false).await?;
                        metrics.acked_lsn.store(m.lsn, Ordering::Relaxed);
                    }
                }
            }
            msg = repl.next_replication_message() => {
                match msg? {
                    ReplMsg::XLogData { wal_start, payload } => {
                        let end = commit_end_lsn(&payload);
                        let txn_complete = end.is_some();
                        let lsn_end = end.unwrap_or(wal_start);
                        metrics.stream_lsn.fetch_max(lsn_end, Ordering::Relaxed);
                        log.append(wal_start, lsn_end, txn_complete, payload).await?;
                        metrics.frames.fetch_add(1, Ordering::Relaxed);
                        if txn_complete {
                            metrics.commits.fetch_add(1, Ordering::Relaxed);
                        }
                        metrics
                            .spilled_frames
                            .store(log.spilled_frames, Ordering::Relaxed);
                        let m = log.durable_now();
                        if m.valid {
                            metrics.durable_lsn.store(m.lsn, Ordering::Relaxed);
                        }
                        // Ack promptly on commit-durable, never while stalled.
                        if txn_complete && !log.is_stalled() && m.valid
                            && m.lsn > metrics.acked_lsn.load(Ordering::Relaxed)
                        {
                            repl.send_standby_status(m.lsn, false).await?;
                            metrics.acked_lsn.store(m.lsn, Ordering::Relaxed);
                        }
                    }
                    ReplMsg::Keepalive { wal_end, reply_requested } => {
                        metrics.stream_lsn.fetch_max(wal_end, Ordering::Relaxed);
                        if reply_requested {
                            let m = log.durable_now();
                            // Floor: before anything is durable we may only restate
                            // the start position — never invent progress.
                            let lsn = if m.valid { m.lsn } else { start_lsn };
                            repl.send_standby_status(lsn, false).await?;
                        }
                    }
                }
            }
            _ = status_interval.tick() => {
                let m = log.durable_now();
                let lsn = if m.valid { m.lsn } else { start_lsn };
                if !log.is_stalled() {
                    // request_reply=true: the server answers with a keepalive carrying
                    // its current wal_end, advancing our stream position even when the
                    // source is otherwise idle.
                    repl.send_standby_status(lsn, true).await?;
                    if m.valid {
                        metrics.acked_lsn.store(m.lsn, Ordering::Relaxed);
                    }
                }
            }
        }
    }

    // Final honest status, then release the socket (slot becomes inactive, retained).
    let m = log.durable_now();
    if m.valid {
        repl.send_standby_status(m.lsn, false).await.ok();
    }
    repl.close().await.ok();
    Ok(())
}

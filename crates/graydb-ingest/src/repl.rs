//! Minimal PostgreSQL replication-protocol client (Decision D-001).
//! Exists because frame-level custody (the Amendment A ack invariant) requires owning
//! the wire: we persist raw pgoutput bytes and control Standby Status Update timing
//! ourselves; mainline tokio-postgres does not expose `replication=database`.
//! SP1 surface: startup + auth (trust/cleartext/md5/SCRAM-SHA-256), simple query,
//! IDENTIFY_SYSTEM, CREATE_REPLICATION_SLOT ... (SNAPSHOT 'export').
//! SP2 surface: START_REPLICATION + CopyBoth pump + Standby Status Update. The
//! reader is buffered so `next_replication_message` is cancel-safe at message
//! granularity (safe inside tokio::select!).

use anyhow::{anyhow, bail, Context, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use postgres_protocol::authentication::sasl::{ChannelBinding, ScramSha256, SCRAM_SHA_256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub struct ReplClient {
    stream: TcpStream,
    buf: BytesMut,
}

#[derive(Debug, Clone)]
pub struct SlotSnapshot {
    pub slot_name: String,
    /// LSN0 — the slot's consistent point; the exact-LSN anchor of the initial load.
    pub consistent_point: String,
    pub snapshot_name: String,
    pub output_plugin: String,
}

/// One message from the CopyBoth replication stream.
#[derive(Debug)]
pub enum ReplMsg {
    /// XLogData: raw pgoutput payload starting at wal_start.
    XLogData { wal_start: u64, payload: Bytes },
    /// Primary keepalive; if reply_requested, send a status update promptly.
    Keepalive { wal_end: u64, reply_requested: bool },
}

#[derive(Debug)]
struct BackendMessage {
    tag: u8,
    body: BytesMut,
}

/// "X/Y" hex → u64.
pub fn parse_lsn(s: &str) -> Result<u64> {
    let (hi, lo) = s
        .split_once('/')
        .ok_or_else(|| anyhow!("bad LSN format: {s}"))?;
    Ok((u64::from_str_radix(hi, 16)? << 32) | u64::from_str_radix(lo, 16)?)
}

pub fn format_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFF_FFFF)
}

/// Microseconds since PostgreSQL epoch (2000-01-01 UTC).
fn pg_now_micros() -> i64 {
    let unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    (unix.as_micros() as i64) - 946_684_800_000_000
}

impl ReplClient {
    /// Open a replication-mode connection (`replication=database`).
    pub async fn connect(
        host: &str,
        port: u16,
        user: &str,
        password: &str,
        dbname: &str,
    ) -> Result<Self> {
        let stream = TcpStream::connect((host, port))
            .await
            .with_context(|| format!("tcp connect {host}:{port}"))?;
        stream.set_nodelay(true)?;
        let mut client = ReplClient {
            stream,
            buf: BytesMut::with_capacity(64 * 1024),
        };
        client.startup(user, dbname).await?;
        client.authenticate(user, password).await?;
        Ok(client)
    }

    async fn startup(&mut self, user: &str, dbname: &str) -> Result<()> {
        let params: [(&str, &str); 5] = [
            ("user", user),
            ("database", dbname),
            ("replication", "database"),
            ("application_name", "graydb-ingest"),
            ("client_encoding", "UTF8"),
        ];
        let mut body = BytesMut::new();
        body.put_i32(196_608); // protocol 3.0
        for (k, v) in params {
            body.put_slice(k.as_bytes());
            body.put_u8(0);
            body.put_slice(v.as_bytes());
            body.put_u8(0);
        }
        body.put_u8(0);
        let mut msg = BytesMut::with_capacity(body.len() + 4);
        msg.put_i32(body.len() as i32 + 4);
        msg.put_slice(&body);
        self.stream.write_all(&msg).await?;
        Ok(())
    }

    /// Drive authentication until ReadyForQuery.
    async fn authenticate(&mut self, user: &str, password: &str) -> Result<()> {
        let mut scram: Option<ScramSha256> = None;
        loop {
            let msg = self.read_message().await?;
            match msg.tag {
                b'R' => {
                    let mut body = msg.body;
                    let code = body.get_i32();
                    match code {
                        0 => {} // AuthenticationOk; keep reading until ReadyForQuery
                        3 => {
                            self.send_password(password.as_bytes()).await?;
                        }
                        5 => {
                            // MD5: md5( md5(password + user) + salt ), "md5"-prefixed hex
                            let mut salt = [0u8; 4];
                            body.copy_to_slice(&mut salt);
                            let inner =
                                format!("{:x}", md5::compute(format!("{password}{user}")));
                            let mut outer_input =
                                Vec::with_capacity(inner.len() + salt.len());
                            outer_input.extend_from_slice(inner.as_bytes());
                            outer_input.extend_from_slice(&salt);
                            let outer = format!("md5{:x}", md5::compute(&outer_input));
                            self.send_password(outer.as_bytes()).await?;
                        }
                        10 => {
                            // SASL: pick SCRAM-SHA-256 (no TLS in the spike => no -PLUS)
                            let mechanisms = read_cstr_list(&mut body)?;
                            anyhow::ensure!(
                                mechanisms.iter().any(|m| m == SCRAM_SHA_256),
                                "server offers no SCRAM-SHA-256 (got {mechanisms:?})"
                            );
                            let s = ScramSha256::new(
                                password.as_bytes(),
                                ChannelBinding::unsupported(),
                            );
                            let mut out = BytesMut::new();
                            out.put_slice(SCRAM_SHA_256.as_bytes());
                            out.put_u8(0);
                            out.put_i32(s.message().len() as i32);
                            out.put_slice(s.message());
                            self.send_msg(b'p', &out).await?;
                            scram = Some(s);
                        }
                        11 => {
                            let s = scram
                                .as_mut()
                                .ok_or_else(|| anyhow!("SASLContinue before SASL start"))?;
                            s.update(&body).context("SCRAM continue")?;
                            let msg_bytes = s.message().to_vec();
                            self.send_msg(b'p', &msg_bytes).await?;
                        }
                        12 => {
                            let s = scram
                                .as_mut()
                                .ok_or_else(|| anyhow!("SASLFinal before SASL start"))?;
                            s.finish(&body).context("SCRAM final verification")?;
                        }
                        other => bail!("unsupported auth request code {other}"),
                    }
                }
                b'S' | b'K' | b'N' => {} // ParameterStatus / BackendKeyData / Notice
                b'Z' => return Ok(()),   // ReadyForQuery
                b'E' => bail!("auth error: {}", parse_error(&msg.body)),
                other => bail!("unexpected message tag {:?} during auth", other as char),
            }
        }
    }

    /// Simple-query protocol; returns data rows as text fields.
    pub async fn simple_query(&mut self, sql: &str) -> Result<Vec<Vec<Option<String>>>> {
        let mut body = BytesMut::with_capacity(sql.len() + 1);
        body.put_slice(sql.as_bytes());
        body.put_u8(0);
        self.send_msg(b'Q', &body).await?;

        let mut rows = Vec::new();
        let mut error: Option<String> = None;
        loop {
            let msg = self.read_message().await?;
            match msg.tag {
                b'T' | b'C' | b'S' | b'N' | b'I' => {}
                b'D' => {
                    let mut b = msg.body;
                    let nfields = b.get_i16();
                    let mut row = Vec::with_capacity(nfields as usize);
                    for _ in 0..nfields {
                        let len = b.get_i32();
                        if len < 0 {
                            row.push(None);
                        } else {
                            let bytes = b.split_to(len as usize);
                            row.push(Some(String::from_utf8_lossy(&bytes).into_owned()));
                        }
                    }
                    rows.push(row);
                }
                b'E' => error = Some(parse_error(&msg.body)),
                b'Z' => break,
                other => bail!("unexpected message tag {:?} in simple query", other as char),
            }
        }
        match error {
            Some(e) => bail!("replication query failed: {e} (sql: {sql})"),
            None => Ok(rows),
        }
    }

    /// IDENTIFY_SYSTEM -> (systemid, timeline, current xlogpos, dbname).
    pub async fn identify_system(&mut self) -> Result<(String, String, String, Option<String>)> {
        let rows = self.simple_query("IDENTIFY_SYSTEM").await?;
        let row = rows.first().ok_or_else(|| anyhow!("IDENTIFY_SYSTEM: no row"))?;
        Ok((
            row.first().cloned().flatten().unwrap_or_default(),
            row.get(1).cloned().flatten().unwrap_or_default(),
            row.get(2).cloned().flatten().unwrap_or_default(),
            row.get(3).cloned().flatten(),
        ))
    }

    /// CREATE_REPLICATION_SLOT <slot> LOGICAL pgoutput (SNAPSHOT 'export').
    /// The returned consistent_point is LSN0; snapshot_name feeds SET TRANSACTION SNAPSHOT.
    /// The exported snapshot stays valid only while THIS connection stays open and idle.
    pub async fn create_slot_with_snapshot(&mut self, slot: &str) -> Result<SlotSnapshot> {
        let sql = format!(
            "CREATE_REPLICATION_SLOT {} LOGICAL pgoutput (SNAPSHOT 'export')",
            crate::quote_ident(slot)
        );
        let rows = self.simple_query(&sql).await?;
        let row = rows
            .first()
            .ok_or_else(|| anyhow!("CREATE_REPLICATION_SLOT returned no row"))?;
        let get = |i: usize| -> Result<String> {
            row.get(i)
                .cloned()
                .flatten()
                .ok_or_else(|| anyhow!("CREATE_REPLICATION_SLOT: column {i} null/missing"))
        };
        Ok(SlotSnapshot {
            slot_name: get(0)?,
            consistent_point: get(1)?,
            snapshot_name: get(2)?,
            output_plugin: get(3)?,
        })
    }

    /// Enter the CopyBoth replication stream from `start_lsn` (pgoutput proto v1).
    pub async fn start_replication(
        &mut self,
        slot: &str,
        publication: &str,
        start_lsn: u64,
    ) -> Result<()> {
        let sql = format!(
            "START_REPLICATION SLOT {} LOGICAL {} (proto_version '1', publication_names '{}')",
            crate::quote_ident(slot),
            format_lsn(start_lsn),
            publication.replace('\'', "''")
        );
        let mut body = BytesMut::with_capacity(sql.len() + 1);
        body.put_slice(sql.as_bytes());
        body.put_u8(0);
        self.send_msg(b'Q', &body).await?;
        loop {
            let msg = self.read_message().await?;
            match msg.tag {
                b'W' => return Ok(()), // CopyBothResponse — streaming begins
                b'S' | b'N' | b'C' => {}
                b'E' => bail!("START_REPLICATION failed: {}", parse_error(&msg.body)),
                b'Z' => bail!("START_REPLICATION ended without entering CopyBoth"),
                other => bail!("unexpected tag {:?} awaiting CopyBoth", other as char),
            }
        }
    }

    /// Next message from the replication stream. Cancel-safe: partial network reads
    /// stay buffered in self.buf, so this may be used inside tokio::select!.
    pub async fn next_replication_message(&mut self) -> Result<ReplMsg> {
        loop {
            let msg = self.read_message().await?;
            match msg.tag {
                b'd' => {
                    let mut b = msg.body;
                    anyhow::ensure!(!b.is_empty(), "empty CopyData");
                    match b.get_u8() {
                        b'w' => {
                            let wal_start = b.get_u64();
                            let _wal_end_on_server = b.get_u64();
                            let _send_time = b.get_i64();
                            return Ok(ReplMsg::XLogData {
                                wal_start,
                                payload: b.freeze(),
                            });
                        }
                        b'k' => {
                            let wal_end = b.get_u64();
                            let _ts = b.get_i64();
                            let reply_requested = b.get_u8() == 1;
                            return Ok(ReplMsg::Keepalive {
                                wal_end,
                                reply_requested,
                            });
                        }
                        other => bail!("unknown CopyData kind {:?}", other as char),
                    }
                }
                b'N' | b'S' => {}
                b'E' => bail!("replication stream error: {}", parse_error(&msg.body)),
                b'c' | b'C' | b'Z' => bail!("replication stream ended by server"),
                other => bail!("unexpected tag {:?} in CopyBoth", other as char),
            }
        }
    }

    /// Standby Status Update: THE ack. Callers must pass only the durable mark from
    /// graydb-log — never a received-but-unsynced position (the invariant).
    pub async fn send_standby_status(&mut self, durable_lsn: u64, request_reply: bool) -> Result<()> {
        let mut inner = BytesMut::with_capacity(1 + 8 * 4 + 1);
        inner.put_u8(b'r');
        inner.put_u64(durable_lsn); // written
        inner.put_u64(durable_lsn); // flushed
        inner.put_u64(durable_lsn); // applied (apply is downstream; log IS the write path)
        inner.put_i64(pg_now_micros());
        inner.put_u8(if request_reply { 1 } else { 0 });
        self.send_msg(b'd', &inner).await
    }

    /// Clean shutdown (Terminate). Dropping the connection releases the exported snapshot.
    pub async fn close(mut self) -> Result<()> {
        self.send_msg(b'X', &[]).await?;
        self.stream.shutdown().await.ok();
        Ok(())
    }

    async fn send_password(&mut self, payload: &[u8]) -> Result<()> {
        let mut body = BytesMut::with_capacity(payload.len() + 1);
        body.put_slice(payload);
        body.put_u8(0);
        self.send_msg(b'p', &body).await
    }

    async fn send_msg(&mut self, tag: u8, body: &[u8]) -> Result<()> {
        let mut msg = BytesMut::with_capacity(body.len() + 5);
        msg.put_u8(tag);
        msg.put_i32(body.len() as i32 + 4);
        msg.put_slice(body);
        self.stream.write_all(&msg).await?;
        Ok(())
    }

    /// Buffered message reader: only complete protocol messages leave the buffer.
    async fn read_message(&mut self) -> Result<BackendMessage> {
        loop {
            if let Some(msg) = Self::try_parse(&mut self.buf)? {
                return Ok(msg);
            }
            let n = self
                .stream
                .read_buf(&mut self.buf)
                .await
                .context("reading from replication socket")?;
            if n == 0 {
                bail!("replication connection closed by peer");
            }
        }
    }

    fn try_parse(buf: &mut BytesMut) -> Result<Option<BackendMessage>> {
        if buf.len() < 5 {
            return Ok(None);
        }
        let len = (&buf[1..5]).get_i32();
        anyhow::ensure!(len >= 4, "invalid message length {len}");
        let total = 1 + len as usize;
        if buf.len() < total {
            return Ok(None);
        }
        let mut msg = buf.split_to(total);
        let tag = msg.get_u8();
        msg.advance(4);
        Ok(Some(BackendMessage { tag, body: msg }))
    }
}

fn read_cstr_list(buf: &mut BytesMut) -> Result<Vec<String>> {
    let mut out = Vec::new();
    loop {
        let nul = buf
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| anyhow!("unterminated string in SASL mechanism list"))?;
        if nul == 0 {
            buf.advance(1);
            return Ok(out);
        }
        let s = buf.split_to(nul);
        buf.advance(1);
        out.push(String::from_utf8_lossy(&s).into_owned());
    }
}

fn parse_error(body: &[u8]) -> String {
    let mut severity = String::new();
    let mut code = String::new();
    let mut message = String::new();
    let mut i = 0;
    while i < body.len() && body[i] != 0 {
        let field = body[i];
        i += 1;
        let start = i;
        while i < body.len() && body[i] != 0 {
            i += 1;
        }
        let val = String::from_utf8_lossy(&body[start..i]).into_owned();
        i += 1;
        match field {
            b'S' => severity = val,
            b'C' => code = val,
            b'M' => message = val,
            _ => {}
        }
    }
    format!("{severity} {code}: {message}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsn_roundtrip() {
        for s in ["0/1FC3768", "A/0", "FFFFFFFF/FFFFFFFF", "0/0"] {
            assert_eq!(format_lsn(parse_lsn(s).unwrap()), s);
        }
        assert_eq!(parse_lsn("0/1FC3768").unwrap(), 0x01FC_3768);
        assert_eq!(parse_lsn("2/10").unwrap(), 0x2_0000_0010);
    }
}

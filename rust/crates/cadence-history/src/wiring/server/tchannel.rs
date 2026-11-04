//! Thrift TChannel History server (TChannel v2 framing, `as=thrift`).
//! Port of the yarpc thrift History handler in go/service/history/wrappers/thrift.
//!
//! Served methods (those with Thrift converters in `cadence-thrift`): Meta::health,
//! CloseShard, RemoveTask, ResetQueue, DescribeQueue. Others answer with a
//! TChannel `BadRequest` error frame (code 0x01 reserved for timeouts; 6 = bad request)
//! until their converters land.
use super::{errors, ServerState};
use anyhow::{bail, Result};
use cadence_thrift::protocol::{Reader, Writer};
use cadence_thrift::value::{read_struct, write_struct, FromThrift, Struct, ToThrift, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, Mutex};

const HDR: usize = 16;
const MAX_PAYLOAD: usize = u16::MAX as usize - HDR;
const INIT_REQ: u8 = 0x01;
const INIT_RES: u8 = 0x02;
const CALL_REQ: u8 = 0x03;
const CALL_RES: u8 = 0x04;
const CONT_REQ: u8 = 0x13;
const CONT_RES: u8 = 0x14;
const PING_REQ: u8 = 0xd0;
const PING_RES: u8 = 0xd1;
const ERROR: u8 = 0xff;
pub const SERVICE_NAME: &str = "cadence-history";

struct Frame {
    kind: u8,
    id: u32,
    payload: Vec<u8>,
}

async fn read_frame<R: AsyncReadExt + Unpin>(r: &mut R) -> Result<Frame> {
    let mut h = [0u8; HDR];
    r.read_exact(&mut h).await?;
    let size = u16::from_be_bytes([h[0], h[1]]) as usize;
    if size < HDR {
        bail!("frame smaller than header");
    }
    let mut payload = vec![0; size - HDR];
    r.read_exact(&mut payload).await?;
    Ok(Frame { kind: h[2], id: u32::from_be_bytes([h[4], h[5], h[6], h[7]]), payload })
}

async fn write_frame(w: &Mutex<OwnedWriteHalf>, f: Frame) -> Result<()> {
    let size = u16::try_from(HDR + f.payload.len())?;
    let mut h = [0u8; HDR];
    h[..2].copy_from_slice(&size.to_be_bytes());
    h[2] = f.kind;
    h[4..8].copy_from_slice(&f.id.to_be_bytes());
    let mut w = w.lock().await;
    w.write_all(&h).await?;
    w.write_all(&f.payload).await?;
    Ok(())
}

fn put16(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u16).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
}

struct Cur<'a>(&'a [u8]);
impl<'a> Cur<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.0.len() < n {
            bail!("short frame");
        }
        let (a, b) = self.0.split_at(n);
        self.0 = b;
        Ok(a)
    }
    fn u8(&mut self) -> Result<u8> { Ok(self.take(1)?[0]) }
    fn u16(&mut self) -> Result<u16> { let b = self.take(2)?; Ok(u16::from_be_bytes([b[0], b[1]])) }
    fn u32(&mut self) -> Result<u32> { let b = self.take(4)?; Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]])) }
    fn s8(&mut self) -> Result<String> { let n = self.u8()? as usize; Ok(String::from_utf8(self.take(n)?.to_vec())?) }
    fn s16(&mut self) -> Result<String> { let n = self.u16()? as usize; Ok(String::from_utf8(self.take(n)?.to_vec())?) }
    fn headers16(&mut self) -> Result<HashMap<String, String>> {
        let n = self.u16()?;
        let mut m = HashMap::new();
        for _ in 0..n {
            let k = self.s16()?;
            let v = self.s16()?;
            m.insert(k, v);
        }
        Ok(m)
    }
}

fn csum_len(kind: u8) -> Result<usize> {
    match kind { 0 => Ok(0), 1 | 2 | 3 => Ok(4), _ => bail!("unsupported checksum type {kind}") }
}

struct Call {
    started: Instant,
    ttl: Duration,
    trace: [u8; 25],
    service: String,
    headers: HashMap<String, String>,
    args: [Vec<u8>; 3],
    arg: usize,
    size: usize,
}

impl Call {
    fn parse_chunks(&mut self, flags: u8, cur: &mut Cur<'_>, limit: usize) -> Result<bool> {
        let kind = cur.u8()?;
        cur.take(csum_len(kind)?)?;
        let mut first = true;
        while !cur.0.is_empty() {
            if !first {
                self.arg += 1;
            }
            first = false;
            if self.arg >= 3 {
                bail!("more than three arguments");
            }
            let n = cur.u16()? as usize;
            let b = cur.take(n)?;
            self.size += n;
            if self.size > limit {
                bail!("message exceeds limit");
            }
            self.args[self.arg].extend_from_slice(b);
        }
        let more = flags & 1 != 0;
        if !more && self.arg != 2 {
            bail!("call missing arguments");
        }
        Ok(more)
    }
    fn initial(payload: &[u8], limit: usize) -> Result<(Self, bool)> {
        let mut c = Cur(payload);
        let flags = c.u8()?;
        let ttl = Duration::from_millis(u64::from(c.u32()?));
        let trace: [u8; 25] = c.take(25)?.try_into()?;
        let service = c.s8()?;
        let n = c.u8()?;
        let mut headers = HashMap::new();
        for _ in 0..n {
            let k = c.s8()?;
            let v = c.s8()?;
            headers.insert(k, v);
        }
        let mut call = Call { started: Instant::now(), ttl, trace, service, headers, args: Default::default(), arg: 0, size: 0 };
        let more = call.parse_chunks(flags, &mut c, limit)?;
        Ok((call, more))
    }
    fn cont(&mut self, payload: &[u8], limit: usize) -> Result<bool> {
        let mut c = Cur(payload);
        let flags = c.u8()?;
        self.parse_chunks(flags, &mut c, limit)
    }
}

async fn send_error(w: &Mutex<OwnedWriteHalf>, id: u32, code: u8, trace: &[u8; 25], msg: &str) -> Result<()> {
    let mut p = vec![code];
    p.extend_from_slice(trace);
    let m = &msg.as_bytes()[..msg.len().min(60000)];
    p.extend_from_slice(&(m.len() as u16).to_be_bytes());
    p.extend_from_slice(m);
    write_frame(w, Frame { kind: ERROR, id, payload: p }).await
}

/// Encode a call response (checksum none) split into frames.
fn response_frames(id: u32, trace: [u8; 25], app_error: bool, arg2: Vec<u8>, arg3: Vec<u8>) -> Vec<Frame> {
    // First frame: flags, code, trace, transport headers (as=thrift), csum type, args.
    let mut head = vec![0u8, u8::from(app_error)];
    head.extend_from_slice(&trace);
    head.extend_from_slice(&[1, 2, b'a', b's', 6, b't', b'h', b'r', b'i', b'f', b't']);
    head.push(0);

    let mut chunks: Vec<(usize, Vec<u8>)> = Vec::new(); // (arg index, data)
    for (i, a) in [Vec::new(), arg2, arg3].into_iter().enumerate() {
        chunks.push((i, a));
    }

    let mut frames = Vec::new();
    let mut cur_payload = head;
    let mut first = true;
    let mut last_arg: Option<usize> = None;
    for (idx, data) in chunks {
        let mut off = 0;
        let mut empty_pending = data.is_empty();
        while off < data.len() || empty_pending {
            let room = MAX_PAYLOAD.saturating_sub(cur_payload.len());
            if room <= 2 {
                cur_payload[0] = 1;
                frames.push((first, std::mem::replace(&mut cur_payload, vec![1, 0])));
                first = false;
                continue;
            }
            let n = (room - 2).min(data.len() - off);
            cur_payload.extend_from_slice(&(n as u16).to_be_bytes());
            cur_payload.extend_from_slice(&data[off..off + n]);
            off += n;
            empty_pending = false;
            last_arg = Some(idx);
            if off < data.len() {
                cur_payload[0] = 1;
                frames.push((first, std::mem::replace(&mut cur_payload, vec![1, 0])));
                first = false;
            }
        }
    }
    let _ = last_arg;
    cur_payload[0] = 0;
    frames.push((first, cur_payload));
    frames
        .into_iter()
        .map(|(first, mut payload)| {
            if !first && payload.len() >= 2 {
                // continuation frames: flags, csum type
                payload[1] = 0;
            }
            Frame { kind: if first { CALL_RES } else { CONT_RES }, id, payload }
        })
        .collect()
}

fn headers16(h: &[(&str, String)]) -> Vec<u8> {
    let mut o = (h.len() as u16).to_be_bytes().to_vec();
    for (k, v) in h {
        put16(&mut o, k);
        put16(&mut o, v);
    }
    o
}

/// Outcome of dispatching one Thrift method.
pub enum Outcome {
    Ok(Struct),
    /// Declared exception: (name, field id, message).
    Exception(&'static str, i16, String),
}

fn exception_id(name: &str) -> i16 {
    match name {
        "BadRequestError" => 1,
        "InternalServiceError" => 2,
        "AccessDeniedError" => 3,
        _ => 2,
    }
}

fn fail(e: &crate::engine::deps::HistoryError) -> Outcome {
    let (name, msg) = errors::thrift_exception(e);
    // Declared exceptions of admin-style history methods: 1 BadRequest, 2 Internal, 3 AccessDenied.
    let name = match name {
        "BadRequestError" | "AccessDeniedError" => name,
        _ => "InternalServiceError",
    };
    Outcome::Exception(name, exception_id(name), msg)
}

fn result_ok(body: Option<Struct>) -> Outcome {
    let mut s = Struct::default();
    if let Some(b) = body {
        s.fields.push((0, Value::Struct(b)));
    }
    Outcome::Ok(s)
}

/// Dispatch a decoded History Thrift call. Returns `Err(message)` for methods
/// that cannot be served (maps to a TChannel bad-request error frame).
pub async fn dispatch(state: &ServerState, procedure: &str, args: &Struct) -> Result<Outcome, String> {
    let method = procedure.rsplit("::").next().unwrap_or(procedure);
    if method == "health" {
        let h = state.handler.health().await.map_err(|e| e.to_string())?;
        let mut s = Struct::default();
        let mut inner = Struct::default();
        inner.fields.push((1, Value::Bool(h.ok)));
        inner.fields.push((2, Value::Bytes(h.msg.into_bytes())));
        s.fields.push((0, Value::Struct(inner)));
        return Ok(Outcome::Ok(s));
    }
    if !state.limiter.allow() {
        let (name, msg) = errors::thrift_exception(&errors::busy());
        return Ok(Outcome::Exception(name, 2, msg));
    }
    let req = match args.get(1) {
        Some(Value::Struct(s)) => s.clone(),
        _ => Struct::default(),
    };
    let dec = |e: cadence_thrift::Error| e.to_string();
    Ok(match method {
        "CloseShard" => {
            let r = cadence_api::admin::CloseShardRequest::from_thrift(&req).map_err(dec)?;
            match state.handler.close_shard(Some(&r)).await { Ok(()) => result_ok(None), Err(e) => fail(&e) }
        }
        "RemoveTask" => {
            let r = cadence_api::admin::RemoveTaskRequest::from_thrift(&req).map_err(dec)?;
            match state.handler.remove_task(Some(&r)).await { Ok(()) => result_ok(None), Err(e) => fail(&e) }
        }
        "ResetQueue" => {
            let r = cadence_api::admin::ResetQueueRequest::from_thrift(&req).map_err(dec)?;
            match state.handler.reset_queue(Some(&r)).await { Ok(()) => result_ok(None), Err(e) => fail(&e) }
        }
        "DescribeQueue" => {
            let r = cadence_api::admin::DescribeQueueRequest::from_thrift(&req).map_err(dec)?;
            match state.handler.describe_queue(Some(&r)).await { Ok(v) => result_ok(Some(v.to_thrift())), Err(e) => fail(&e) }
        }
        other => return Err(format!("HistoryService::{other} is not implemented yet")),
    })
}

async fn handle_call(w: Arc<Mutex<OwnedWriteHalf>>, id: u32, call: Call, state: ServerState) -> Result<()> {
    if call.service != SERVICE_NAME {
        return send_error(&w, id, 6, &call.trace, "Unknown TChannel service").await;
    }
    if call.headers.get("as").map(String::as_str) != Some("thrift") {
        return send_error(&w, id, 6, &call.trace, "Cadence TChannel requires thrift encoding").await;
    }
    let procedure = std::str::from_utf8(&call.args[0])?.to_string();
    let mut hc = Cur(&call.args[1]);
    let _app_headers = hc.headers16()?;
    let mut rd = Reader::new(&call.args[2]);
    let args = read_struct(&mut rd)?;
    let remaining = (call.started + call.ttl).saturating_duration_since(Instant::now());
    let res = tokio::time::timeout(remaining, dispatch(&state, &procedure, &args)).await;
    let outcome = match res {
        Err(_) => return send_error(&w, id, 1, &call.trace, "Request timed out").await,
        Ok(Err(m)) => return send_error(&w, id, 6, &call.trace, &m).await,
        Ok(Ok(o)) => o,
    };
    let mut meta = vec![("$rpc$-service", SERVICE_NAME.to_string())];
    let (result, app) = match outcome {
        Outcome::Ok(s) => (s, false),
        Outcome::Exception(name, fid, msg) => {
            meta.push(("$rpc$-application-error-name", name.to_string()));
            meta.push(("$rpc$-application-error-details", msg.chars().take(200).collect()));
            let mut ex = Struct::default();
            ex.fields.push((1, Value::Bytes(msg.into_bytes())));
            let mut s = Struct::default();
            s.fields.push((fid, Value::Struct(ex)));
            (s, true)
        }
    };
    let mut body = Writer::new();
    write_struct(&mut body, &result);
    for f in response_frames(id, call.trace, app, headers16(&meta), body.into_bytes()) {
        write_frame(&w, f).await?;
    }
    Ok(())
}

async fn connection(stream: TcpStream, state: ServerState) -> Result<()> {
    stream.set_nodelay(true)?;
    let local = stream.local_addr()?.to_string();
    let (mut rd, wr) = stream.into_split();
    let w = Arc::new(Mutex::new(wr));
    let init = tokio::time::timeout(Duration::from_secs(10), read_frame(&mut rd)).await??;
    if init.kind != INIT_REQ {
        bail!("connection must start with init");
    }
    let mut c = Cur(&init.payload);
    if c.u16()? != 2 {
        bail!("unsupported protocol version");
    }
    let h = c.headers16()?;
    if !h.contains_key("host_port") || !h.contains_key("process_name") {
        bail!("missing init headers");
    }
    let mut p = 2u16.to_be_bytes().to_vec();
    p.extend(headers16(&[
        ("host_port", local),
        ("process_name", "cadence-history".to_string()),
        ("tchannel_language", "rust".to_string()),
        ("tchannel_language_version", env!("CARGO_PKG_VERSION").to_string()),
        ("tchannel_version", env!("CARGO_PKG_VERSION").to_string()),
    ]));
    write_frame(&w, Frame { kind: INIT_RES, id: init.id, payload: p }).await?;
    let mut calls: HashMap<u32, Call> = HashMap::new();
    loop {
        let f = read_frame(&mut rd).await?;
        match f.kind {
            PING_REQ => write_frame(&w, Frame { kind: PING_RES, id: f.id, payload: vec![] }).await?,
            CALL_REQ | CONT_REQ => {
                let done = if f.kind == CALL_REQ {
                    let (call, more) = Call::initial(&f.payload, state.max_message_size)?;
                    calls.insert(f.id, call);
                    !more
                } else {
                    match calls.get_mut(&f.id) {
                        Some(call) => !call.cont(&f.payload, state.max_message_size)?,
                        None => continue,
                    }
                };
                if done {
                    let call = calls.remove(&f.id).expect("call");
                    let (w, st, id) = (w.clone(), state.clone(), f.id);
                    tokio::spawn(async move {
                        let trace = call.trace;
                        if let Err(e) = handle_call(w.clone(), id, call, st).await {
                            let _ = send_error(&w, id, 6, &trace, &e.to_string()).await;
                        }
                    });
                }
            }
            0xc0 => {
                calls.remove(&f.id);
            }
            _ => bail!("unsupported frame type {:#x}", f.kind),
        }
    }
}

pub async fn serve(listener: TcpListener, state: ServerState, mut stop: watch::Receiver<bool>) {
    loop {
        tokio::select! {
            _ = stop.wait_for(|v| *v) => return,
            acc = listener.accept() => {
                if let Ok((s, _)) = acc {
                    let st = state.clone();
                    let mut stop = stop.clone();
                    tokio::spawn(async move {
                        tokio::select! {
                            _ = connection(s, st) => {},
                            _ = stop.wait_for(|v| *v) => {},
                        }
                    });
                }
            }
        }
    }
}

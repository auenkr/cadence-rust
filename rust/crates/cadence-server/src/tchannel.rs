//! TChannel v2 Thrift frontend for SDKs using Cadence's default transport.
//! Frame, argument, and checksum semantics follow the TChannel specification
//! and github.com/uber/tchannel-go v1.34.4. Calls share the Rust YARPC dispatcher.
//! No Go process or HTTP proxy is used.
use crate::handlers::AppState;
use anyhow::{anyhow, bail, Context, Result};
use cadence_thrift::{
    protocol::{Reader, Writer},
    value::{read_struct, write_struct},
};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{tcp::OwnedWriteHalf, TcpListener, TcpStream},
    sync::Mutex,
    task::JoinSet,
};

const FRAME_HEADER: usize = 16;
const FRAME_PAYLOAD: usize = u16::MAX as usize - FRAME_HEADER;
const INIT_REQ: u8 = 0x01;
const INIT_RES: u8 = 0x02;
const CALL_REQ: u8 = 0x03;
const CALL_RES: u8 = 0x04;
const CONT_REQ: u8 = 0x13;
const CONT_RES: u8 = 0x14;
const CANCEL: u8 = 0xc0;
const PING_REQ: u8 = 0xd0;
const PING_RES: u8 = 0xd1;
const ERROR: u8 = 0xff;
type SharedWriter = Arc<Mutex<OwnedWriteHalf>>;

struct Cursor<'a> {
    data: &'a [u8],
    position: usize,
}
impl<'a> Cursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, position: 0 }
    }
    fn take(&mut self, size: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(size)
            .ok_or_else(|| anyhow!("TChannel length overflow"))?;
        let bytes = self
            .data
            .get(self.position..end)
            .ok_or_else(|| anyhow!("Truncated TChannel payload"))?;
        self.position = end;
        Ok(bytes)
    }
    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn short(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into()?))
    }
    fn int(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into()?))
    }
    fn string8(&mut self) -> Result<String> {
        let size = self.byte()? as usize;
        Ok(std::str::from_utf8(self.take(size)?)?.to_string())
    }
    fn string16(&mut self) -> Result<String> {
        let size = self.short()? as usize;
        Ok(std::str::from_utf8(self.take(size)?)?.to_string())
    }
    fn remaining(&self) -> usize {
        self.data.len() - self.position
    }
    fn headers8(&mut self) -> Result<HashMap<String, String>> {
        let count = self.byte()?;
        let mut headers = HashMap::new();
        for _ in 0..count {
            let key = self.string8()?;
            let value = self.string8()?;
            headers.insert(key, value);
        }
        Ok(headers)
    }
    fn headers16(&mut self) -> Result<HashMap<String, String>> {
        let count = self.short()?;
        let mut headers = HashMap::new();
        for _ in 0..count {
            let key = self.string16()?;
            let value = self.string16()?;
            headers.insert(key, value);
        }
        Ok(headers)
    }
}
fn string16(out: &mut Vec<u8>, text: &str) -> Result<()> {
    out.extend_from_slice(&u16::try_from(text.len())?.to_be_bytes());
    out.extend_from_slice(text.as_bytes());
    Ok(())
}
fn headers16(headers: &[(&str, String)]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(&u16::try_from(headers.len())?.to_be_bytes());
    for (key, value) in headers {
        string16(&mut out, key)?;
        string16(&mut out, value)?;
    }
    Ok(out)
}
fn transport_headers() -> Vec<u8> {
    vec![1, 2, b'a', b's', 6, b't', b'h', b'r', b'i', b'f', b't']
}

#[derive(Clone)]
enum Checksum {
    None,
    Ieee(crc32fast::Hasher),
    Castagnoli(u32),
}
impl Checksum {
    fn new(kind: u8) -> Result<Self> {
        match kind {
            0 => Ok(Self::None),
            1 => Ok(Self::Ieee(crc32fast::Hasher::new())),
            3 => Ok(Self::Castagnoli(0)),
            _ => bail!("Unsupported TChannel checksum type {kind}"),
        }
    }
    fn kind(&self) -> u8 {
        match self {
            Self::None => 0,
            Self::Ieee(_) => 1,
            Self::Castagnoli(_) => 3,
        }
    }
    fn size(&self) -> usize {
        if matches!(self, Self::None) {
            0
        } else {
            4
        }
    }
    fn add(&mut self, data: &[u8]) {
        match self {
            Self::None => {}
            Self::Ieee(h) => h.update(data),
            Self::Castagnoli(v) => *v = crc32c::crc32c_append(*v, data),
        }
    }
    fn value(&self) -> Vec<u8> {
        match self {
            Self::None => Vec::new(),
            Self::Ieee(h) => h.clone().finalize().to_be_bytes().to_vec(),
            Self::Castagnoli(v) => v.to_be_bytes().to_vec(),
        }
    }
}
struct Frame {
    kind: u8,
    id: u32,
    payload: Vec<u8>,
}
async fn read_frame(reader: &mut tokio::net::tcp::OwnedReadHalf) -> Result<Frame> {
    let mut header = [0; FRAME_HEADER];
    reader.read_exact(&mut header).await?;
    let size = u16::from_be_bytes(header[..2].try_into()?) as usize;
    if size < FRAME_HEADER {
        bail!("TChannel frame is smaller than its header");
    }
    let mut payload = vec![0; size - FRAME_HEADER];
    reader.read_exact(&mut payload).await?;
    Ok(Frame {
        kind: header[2],
        id: u32::from_be_bytes(header[4..8].try_into()?),
        payload,
    })
}
async fn write_frame(writer: &mut OwnedWriteHalf, frame: Frame) -> Result<()> {
    let size = u16::try_from(FRAME_HEADER + frame.payload.len())?;
    let mut header = [0; FRAME_HEADER];
    header[..2].copy_from_slice(&size.to_be_bytes());
    header[2] = frame.kind;
    header[4..8].copy_from_slice(&frame.id.to_be_bytes());
    writer.write_all(&header).await?;
    writer.write_all(&frame.payload).await?;
    Ok(())
}
async fn error(
    writer: &SharedWriter,
    id: u32,
    code: u8,
    trace: &[u8; 25],
    message: &str,
) -> Result<()> {
    let mut payload = vec![code];
    payload.extend_from_slice(trace);
    let mut end = message.len().min(65500);
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    string16(&mut payload, &message[..end])?;
    write_frame(
        &mut *writer.lock().await,
        Frame {
            kind: ERROR,
            id,
            payload,
        },
    )
    .await
}

struct Call {
    started: Instant,
    ttl: Duration,
    trace: [u8; 25],
    service: String,
    headers: HashMap<String, String>,
    checksum: Checksum,
    args: [Vec<u8>; 3],
    arg: usize,
    size: usize,
}
impl Call {
    fn initial(payload: &[u8], limit: usize) -> Result<(Self, bool)> {
        let mut input = Cursor::new(payload);
        let flags = input.byte()?;
        let ttl = Duration::from_millis(u64::from(input.int()?));
        let trace = input.take(25)?.try_into()?;
        let service = input.string8()?;
        let headers = input.headers8()?;
        let kind = input.byte()?;
        let mut call = Self {
            started: Instant::now(),
            ttl,
            trace,
            service,
            headers,
            checksum: Checksum::new(kind)?,
            args: Default::default(),
            arg: 0,
            size: 0,
        };
        let more = call.chunks(flags, kind, &mut input, limit)?;
        Ok((call, more))
    }
    fn continuation(&mut self, payload: &[u8], limit: usize) -> Result<bool> {
        let mut input = Cursor::new(payload);
        let flags = input.byte()?;
        let kind = input.byte()?;
        self.chunks(flags, kind, &mut input, limit)
    }
    fn chunks(
        &mut self,
        flags: u8,
        kind: u8,
        input: &mut Cursor<'_>,
        limit: usize,
    ) -> Result<bool> {
        if flags & !1 != 0 {
            bail!("Unsupported TChannel call flags");
        }
        if self.checksum.kind() != kind {
            bail!("TChannel checksum type changed between fragments");
        }
        let expected = input.take(self.checksum.size())?.to_vec();
        let mut first = true;
        while input.remaining() > 0 {
            if !first {
                self.arg += 1;
            }
            first = false;
            if self.arg >= 3 {
                bail!("TChannel call has more than three arguments");
            }
            let size = input.short()? as usize;
            let bytes = input.take(size)?;
            self.size = self
                .size
                .checked_add(size)
                .ok_or_else(|| anyhow!("TChannel call size overflow"))?;
            if self.size > limit {
                bail!("TChannel call exceeds configured message limit");
            }
            self.checksum.add(bytes);
            self.args[self.arg].extend_from_slice(bytes);
        }
        if first {
            bail!("TChannel fragment contains no argument chunk");
        }
        if self.checksum.value() != expected {
            bail!("TChannel checksum mismatch");
        }
        let more = flags & 1 != 0;
        if !more && self.arg != 2 {
            bail!("TChannel call is missing arguments");
        }
        Ok(more)
    }
}

/// Serialize arguments with the same empty continuation marker as tchannel-go.
fn response_frames(
    id: u32,
    trace: [u8; 25],
    application_error: bool,
    args: [Vec<u8>; 3],
    mut checksum: Checksum,
) -> Result<Vec<Frame>> {
    let mut first = true;
    let mut frames = Vec::new();
    let mut arg = 0;
    let mut offset = 0;
    let mut marker = false;
    loop {
        let mut payload = vec![0];
        if first {
            payload.push(u8::from(application_error));
            payload.extend_from_slice(&trace);
            payload.extend_from_slice(&transport_headers());
        }
        payload.push(checksum.kind());
        let checksum_at = payload.len();
        payload.resize(payload.len() + checksum.size(), 0);
        if marker {
            payload.extend_from_slice(&0u16.to_be_bytes());
            marker = false;
        }
        let mut done = false;
        loop {
            let room = FRAME_PAYLOAD - payload.len();
            if room <= 2 {
                break;
            }
            let count = (room - 2).min(args[arg].len() - offset);
            payload.extend_from_slice(&u16::try_from(count)?.to_be_bytes());
            let data = &args[arg][offset..offset + count];
            checksum.add(data);
            payload.extend_from_slice(data);
            offset += count;
            if offset < args[arg].len() {
                break;
            }
            if arg == 2 {
                done = true;
                break;
            }
            arg += 1;
            offset = 0;
            if FRAME_PAYLOAD - payload.len() <= 2 {
                marker = true;
                break;
            }
        }
        payload[0] = u8::from(!done);
        payload[checksum_at..checksum_at + checksum.size()].copy_from_slice(&checksum.value());
        frames.push(Frame {
            kind: if first { CALL_RES } else { CONT_RES },
            id,
            payload,
        });
        first = false;
        if done {
            return Ok(frames);
        }
    }
}
async fn handle_call(writer: SharedWriter, id: u32, call: Call, state: AppState, matching: Option<crate::grpc_matching::Matching>) -> Result<()> {
    let expected_service = if matching.is_some() { "cadence-matching" } else { "cadence-frontend" };
    if call.service != expected_service {
        return error(&writer, id, 6, &call.trace, "Unknown TChannel service").await;
    }
    if call.headers.get("as").map(String::as_str) != Some("thrift") {
        return error(
            &writer,
            id,
            6,
            &call.trace,
            "Cadence TChannel requires thrift encoding",
        )
        .await;
    }
    let procedure = std::str::from_utf8(&call.args[0])?;
    if !procedure.contains("::") {
        bail!("Unknown Cadence procedure");
    }
    let definition = cadence_thrift::service::find_method(procedure)
        .filter(|definition| definition.service == "Meta" || definition.service == if matching.is_some() { "MatchingService" } else { "WorkflowService" })
        .ok_or_else(|| anyhow!("Unknown Cadence procedure"))?;
    let method = definition.method;
    let mut headers = Cursor::new(&call.args[1]);
    headers.headers16()?;
    if headers.remaining() != 0 {
        bail!("Trailing TChannel application headers");
    }
    let mut reader = Reader::new(&call.args[2]);
    let request = read_struct(&mut reader)?;
    let deadline = call.started + call.ttl;
    let remaining = deadline.saturating_duration_since(Instant::now());
    let result = tokio::time::timeout(
        remaining,
        async {
            match matching {
                Some(matching) => crate::thrift_matching::dispatch(&matching, procedure, &request, deadline).await,
                None => crate::yarpc::dispatch(&state, method, &request, deadline.min(Instant::now() + Duration::from_secs(60))).await,
            }
        },
    )
    .await;
    let (result, application_error) = match result {
        Err(_) => return error(&writer, id, 1, &call.trace, "Request timed out").await,
        Ok(Err(message)) => return error(&writer, id, 6, &call.trace, &message).await,
        Ok(Ok(value)) => value,
    };
    let mut meta = vec![("$rpc$-service", expected_service.to_string())];
    if let Some((name, details)) = &application_error {
        meta.push(("$rpc$-application-error-name", name.to_string()));
        let mut end = details.len().min(256);
        while !details.is_char_boundary(end) {
            end -= 1;
        }
        meta.push((
            "$rpc$-application-error-details",
            details[..end].to_string(),
        ));
    }
    let mut body = Writer::new();
    write_struct(&mut body, &result);
    let frames = response_frames(
        id,
        call.trace,
        application_error.is_some(),
        [Vec::new(), headers16(&meta)?, body.into_bytes()],
        Checksum::new(call.checksum.kind())?,
    )?;
    let mut writer = writer.lock().await;
    for frame in frames {
        write_frame(&mut writer, frame).await?;
    }
    Ok(())
}

async fn connection(stream: TcpStream, state: AppState, matching: Option<crate::grpc_matching::Matching>) -> Result<()> {
    stream.set_nodelay(true)?;
    let local = stream.local_addr()?.to_string();
    let (mut reader, writer) = stream.into_split();
    let writer = Arc::new(Mutex::new(writer));
    let init = tokio::time::timeout(Duration::from_secs(10), read_frame(&mut reader)).await??;
    if init.kind != INIT_REQ {
        bail!("TChannel connection must start with init");
    }
    let mut input = Cursor::new(&init.payload);
    if input.short()? != 2 {
        bail!("Unsupported TChannel protocol version");
    }
    let headers = input.headers16()?;
    if !headers.contains_key("host_port") || !headers.contains_key("process_name") {
        bail!("Missing TChannel init headers");
    }
    let mut payload = 2u16.to_be_bytes().to_vec();
    payload.extend_from_slice(&headers16(&[
        ("host_port", local),
        ("process_name", "cadence-rust".into()),
        ("tchannel_language", "rust".into()),
        (
            "tchannel_language_version",
            env!("CARGO_PKG_VERSION").into(),
        ),
        ("tchannel_version", env!("CARGO_PKG_VERSION").into()),
    ])?);
    write_frame(
        &mut *writer.lock().await,
        Frame {
            kind: INIT_RES,
            id: init.id,
            payload,
        },
    )
    .await?;
    let mut calls = HashMap::<u32, Call>::new();
    let mut tasks = JoinSet::new();
    let mut aborts = HashMap::new();
    loop {
        let expiry = calls.values().map(|call| call.started + call.ttl).min();
        let delay = expiry
            .map(|expiry| expiry.saturating_duration_since(Instant::now()))
            .unwrap_or(Duration::from_secs(60));
        tokio::select! {
            _=tokio::time::sleep(delay),if expiry.is_some()=>{
                let expired:Vec<_>=calls.iter().filter(|(_,call)|call.started+call.ttl<=Instant::now()).map(|(id,call)|(*id,call.trace)).collect();
                for (id,trace) in expired {calls.remove(&id);error(&writer,id,1,&trace,"Request timed out while assembling fragments").await?;}
            }
            completed=tasks.join_next(),if !tasks.is_empty()=>{
                if let Some(Ok((id,result)))=completed {aborts.remove(&id);result?;}
            }
            frame=read_frame(&mut reader)=>{
                let frame=frame?;
                match frame.kind {
                    PING_REQ=>write_frame(&mut *writer.lock().await,Frame {kind:PING_RES,id:frame.id,payload:Vec::new()}).await?,
                    CANCEL=>{calls.remove(&frame.id);if let Some(task)=aborts.remove(&frame.id) {tokio::task::AbortHandle::abort(&task);}},
                    CALL_REQ|CONT_REQ=>{
                        let complete=if frame.kind==CALL_REQ {
                            if calls.contains_key(&frame.id)||aborts.contains_key(&frame.id) {bail!("Duplicate TChannel call ID");}
                            let (call,more)=Call::initial(&frame.payload,state.grpc_max_message_size)?;
                            calls.insert(frame.id,call);!more
                        } else {
                            let Some(call)=calls.get_mut(&frame.id) else {continue;};
                            !call.continuation(&frame.payload,state.grpc_max_message_size)?
                        };
                        if complete {
                            let call=calls.remove(&frame.id).expect("assembled call");let id=frame.id;let writer=writer.clone();let state=state.clone();let matching=matching.clone();
                            let abort=tasks.spawn(async move {
                                let trace=call.trace;let result=handle_call(writer.clone(),id,call,state,matching).await;
                                let result=if let Err(e)=result {error(&writer,id,6,&trace,&e.to_string()).await} else {Ok(())};
                                (id,result)
                            });aborts.insert(id,abort);
                        }
                    }
                    ERROR=>bail!("Peer sent a TChannel error"),
                    _=>{error(&writer,frame.id,0xff,&[0;25],"Unsupported TChannel frame type").await?;bail!("Unsupported TChannel frame type");}
                }
            }
        }
    }
}
pub async fn serve(addr: std::net::SocketAddr, state: AppState) -> Result<()> {
    serve_with(addr, state, None).await
}
pub async fn serve_matching(addr: std::net::SocketAddr, matching: crate::grpc_matching::Matching) -> Result<()> {
    serve_with(addr, matching.frontend.state.clone(), Some(matching)).await
}
async fn serve_with(addr: std::net::SocketAddr, state: AppState, matching: Option<crate::grpc_matching::Matching>) -> Result<()> {
    let listener = TcpListener::bind(addr)
        .await
        .context("bind TChannel frontend")?;
    loop {
        let (stream, _) = listener.accept().await?;
        let state = state.clone();
        let matching = matching.clone();
        tokio::spawn(async move {
            let _ = connection(stream, state, matching).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn assemble(frames: Vec<Frame>, kind: u8) -> Result<[Vec<u8>; 3]> {
        let mut call = Call {
            started: Instant::now(),
            ttl: Duration::from_secs(1),
            trace: [0; 25],
            service: String::new(),
            headers: HashMap::new(),
            checksum: Checksum::new(kind)?,
            args: Default::default(),
            arg: 0,
            size: 0,
        };
        let length = frames.len();
        for (index, frame) in frames.into_iter().enumerate() {
            let mut input = Cursor::new(&frame.payload);
            let flags = input.byte()?;
            if index == 0 {
                assert_eq!(frame.kind, CALL_RES);
                input.byte()?;
                input.take(25)?;
                input.headers8()?;
            } else {
                assert_eq!(frame.kind, CONT_RES);
            }
            let kind = input.byte()?;
            let more = call.chunks(flags, kind, &mut input, 16 * 1024 * 1024)?;
            assert_eq!(more, index + 1 < length);
        }
        Ok(call.args)
    }
    #[test]
    fn empty_argument_markers_and_fragment_boundaries_round_trip() {
        for kind in [0, 1, 3] {
            let checksum = Checksum::new(kind).unwrap();
            let first_capacity =
                FRAME_PAYLOAD - (1 + 1 + 25 + transport_headers().len() + 1 + checksum.size() + 2);
            let args = [
                vec![b'a'; first_capacity],
                vec![b'b'; 4],
                vec![b'c'; 150000],
            ];
            let frames = response_frames(42, [7; 25], false, args.clone(), checksum).unwrap();
            assert!(frames.len() > 2);
            assert_eq!(assemble(frames, kind).unwrap(), args);
        }
    }
    #[test]
    fn checksum_corruption_is_rejected() {
        let mut frames = response_frames(
            42,
            [0; 25],
            false,
            [vec![], vec![0, 0], b"body".to_vec()],
            Checksum::new(1).unwrap(),
        )
        .unwrap();
        let last = frames[0].payload.len() - 1;
        frames[0].payload[last] ^= 1;
        assert!(assemble(frames, 1)
            .unwrap_err()
            .to_string()
            .contains("checksum mismatch"));
    }
    #[test]
    fn checksum_algorithms_match_standard_reference_vectors() {
        for (kind, expected) in [(1, 0xcbf43926u32), (3, 0xe3069283u32)] {
            let mut checksum = Checksum::new(kind).unwrap();
            checksum.add(b"1234");
            checksum.add(b"56789");
            assert_eq!(checksum.value(), expected.to_be_bytes());
        }
    }
    #[test]
    fn truncated_lengths_fail_without_panicking() {
        assert!(Cursor::new(&[0]).short().is_err());
        assert!(Cursor::new(&[0, 8, b'x']).string16().is_err());
        let mut cursor = Cursor::new(&[0]);
        cursor.take(1).unwrap();
        assert!(cursor.take(usize::MAX).is_err());
    }
}

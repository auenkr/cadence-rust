//! One-shot outbound TChannel v2 JSON calls, compatible with tchannel-go v1.34.4.
//! A call owns its TCP connection; dropping its future drops the socket and therefore
//! cancels the remote call. This is intentionally a transport primitive, not discovery.
use anyhow::{anyhow, bail, Result};
use serde_json::Value;
use std::{collections::HashMap, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};

const MAX_FRAME: usize = u16::MAX as usize;
#[derive(Debug)]
pub struct JsonResponse {
    pub headers: Option<HashMap<String, String>>,
    pub body: Value,
    pub application_error: bool,
}
#[derive(Debug)]
pub enum JsonCallError {
    Application(Value),
    Transport(anyhow::Error),
}
impl std::fmt::Display for JsonCallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Application(v) => write!(f, "TChannel JSON application error: {v}"),
            Self::Transport(e) => write!(f, "{e}"),
        }
    }
}
impl std::error::Error for JsonCallError {}
fn s8(out: &mut Vec<u8>, s: &str) -> Result<()> {
    let n = u8::try_from(s.len())?;
    out.push(n);
    out.extend_from_slice(s.as_bytes());
    Ok(())
}
fn s16(out: &mut Vec<u8>, s: &str) -> Result<()> {
    out.extend_from_slice(&u16::try_from(s.len())?.to_be_bytes());
    out.extend_from_slice(s.as_bytes());
    Ok(())
}
fn headers16(entries: &[(&str, &str)]) -> Result<Vec<u8>> {
    let mut o = vec![];
    o.extend_from_slice(&u16::try_from(entries.len())?.to_be_bytes());
    for (k, v) in entries {
        s16(&mut o, k)?;
        s16(&mut o, v)?;
    }
    Ok(o)
}
async fn write_frame(s: &mut TcpStream, kind: u8, id: u32, p: &[u8]) -> Result<()> {
    let n = u16::try_from(16 + p.len())?;
    let mut h = [0u8; 16];
    h[..2].copy_from_slice(&n.to_be_bytes());
    h[2] = kind;
    h[4..8].copy_from_slice(&id.to_be_bytes());
    s.write_all(&h).await?;
    s.write_all(p).await?;
    Ok(())
}
async fn read_frame(s: &mut TcpStream) -> Result<(u8, u32, Vec<u8>)> {
    let mut h = [0; 16];
    s.read_exact(&mut h).await?;
    let n = u16::from_be_bytes([h[0], h[1]]) as usize;
    if n < 16 {
        bail!("invalid TChannel frame length {n}")
    }
    let mut p = vec![0; n - 16];
    s.read_exact(&mut p).await?;
    Ok((h[2], u32::from_be_bytes(h[4..8].try_into()?), p))
}
fn get16(b: &[u8], p: &mut usize) -> Result<usize> {
    let x = b
        .get(*p..*p + 2)
        .ok_or_else(|| anyhow!("truncated TChannel field"))?;
    *p += 2;
    Ok(u16::from_be_bytes([x[0], x[1]]) as usize)
}
fn read_string(b: &[u8], p: &mut usize) -> Result<String> {
    let n = get16(b, p)?;
    let x = b
        .get(*p..*p + n)
        .ok_or_else(|| anyhow!("truncated TChannel string"))?;
    *p += n;
    Ok(std::str::from_utf8(x)?.to_string())
}

fn read_string8(b: &[u8], p: &mut usize) -> Result<String> {
    let n = *b
        .get(*p)
        .ok_or_else(|| anyhow!("truncated TChannel string"))? as usize;
    *p += 1;
    let x = b
        .get(*p..*p + n)
        .ok_or_else(|| anyhow!("truncated TChannel string"))?;
    *p += n;
    Ok(std::str::from_utf8(x)?.to_string())
}
/// Make one JSON call. `headers: None` is encoded as JSON null, `Some(empty)` as `{}`.
pub async fn call(
    addr: &str,
    service: &str,
    method: &str,
    headers: Option<HashMap<String, String>>,
    request: &Value,
    deadline: Duration,
) -> std::result::Result<JsonResponse, JsonCallError> {
    let work = async {
        let mut s = TcpStream::connect(addr).await?;
        s.set_nodelay(true)?;
        let mut init = 2u16.to_be_bytes().to_vec();
        init.extend(headers16(&[
            ("host_port", "0.0.0.0:0"),
            ("process_name", "cadence-rust-ringpop-client"),
        ])?);
        write_frame(&mut s, 1, 1, &init).await?;
        let (k, _, p) = read_frame(&mut s).await?;
        if k != 2 || p.len() < 4 {
            bail!("invalid TChannel init response")
        }
        if u16::from_be_bytes([p[0], p[1]]) != 2 {
            bail!("unsupported TChannel init response version")
        }
        let mut init_pos = 2;
        let init_headers = get16(&p, &mut init_pos)?;
        for _ in 0..init_headers {
            let _ = read_string(&p, &mut init_pos)?;
            let _ = read_string(&p, &mut init_pos)?;
        }
        if init_pos != p.len() {
            bail!("trailing TChannel init response bytes")
        }
        let mut arg2 = serde_json::to_vec(&headers)?;
        arg2.push(b'\n');
        let mut arg3 = serde_json::to_vec(request)?;
        arg3.push(b'\n');
        let mut head = vec![1];
        head.extend((deadline.as_millis().min(u32::MAX as u128) as u32).to_be_bytes());
        head.extend([0u8; 25]);
        s8(&mut head, service)?;
        head.push(1);
        s8(&mut head, "as")?;
        s8(&mut head, "json")?;
        head.push(0); // checksum none
        let args = [method.as_bytes(), arg2.as_slice(), arg3.as_slice()];
        let mut arg_i = 0usize;
        let mut off = 0usize;
        let mut first = true;
        let mut marker = false;
        let id = 2u32;
        loop {
            let mut chunk = if first { head.clone() } else { vec![0, 0] };
            let cap = MAX_FRAME - 16;
            if marker {
                chunk.extend(0u16.to_be_bytes());
                marker = false;
            }
            while arg_i < 3 {
                let room = cap.saturating_sub(chunk.len());
                if room < 2 {
                    break;
                }
                let n = (room - 2)
                    .min(args[arg_i].len() - off)
                    .min(u16::MAX as usize);
                chunk.extend((n as u16).to_be_bytes());
                chunk.extend(&args[arg_i][off..off + n]);
                off += n;
                if off < args[arg_i].len() {
                    break;
                }
                arg_i += 1;
                off = 0;
                if arg_i < 3 && cap.saturating_sub(chunk.len()) < 2 {
                    marker = true;
                    break;
                }
            }
            let more = arg_i < 3;
            chunk[0] = u8::from(more);
            write_frame(&mut s, if first { 3 } else { 0x13 }, id, &chunk).await?;
            if !more {
                break;
            }
            first = false;
        }
        let (mut app, mut arg_index, mut args_out) =
            (false, 0usize, [Vec::new(), Vec::new(), Vec::new()]);

        let mut first_response = true;
        loop {
            let (k, id, p) = read_frame(&mut s).await?;
            if id != 2 {
                bail!("unexpected TChannel response id {id}")
            };
            if k == 0xff {
                if p.len() < 27 {
                    bail!("malformed TChannel error frame")
                }
                let code = p[0];
                let mut q = 26;
                let msg = read_string(&p, &mut q)?;
                if q != p.len() {
                    bail!("trailing TChannel error bytes")
                }
                bail!("TChannel error {code}: {msg}")
            };
            if (first_response && k != 4) || (!first_response && k != 0x14) {
                bail!("unexpected TChannel response frame {k:#x}")
            };
            first_response = false;
            let mut q = 0;
            let has_more = *p.get(q).ok_or_else(|| anyhow!("empty TChannel response"))?;
            if has_more & !1 != 0 {
                bail!("unsupported TChannel response flags")
            }
            q += 1;
            if k == 4 {
                app = p[q] != 0;
                q += 1 + 25;
                let n = *p.get(q).ok_or_else(|| anyhow!("missing checksum type"))? as usize;
                q += 1;
                for _ in 0..n {
                    let _ = read_string8(p.as_slice(), &mut q)?;
                    let _ = read_string8(p.as_slice(), &mut q)?;
                }
            }
            let ck = *p.get(q).ok_or_else(|| anyhow!("missing checksum"))?;
            q += 1;
            if ck != 0 && ck != 1 && ck != 3 {
                bail!("unsupported TChannel checksum type {ck}")
            }
            let cs = if ck == 0 { 0 } else { 4 };
            if q + cs > p.len() {
                bail!("truncated checksum")
            };
            let expected = &p[q..q + cs];
            q += cs;
            while q < p.len() {
                let n = get16(&p, &mut q)?;
                let bytes = p
                    .get(q..q + n)
                    .ok_or_else(|| anyhow!("truncated response arg"))?;
                q += n;
                args_out[arg_index].extend_from_slice(bytes);
                let has_next_chunk = q < p.len();
                if bytes.is_empty() || has_next_chunk {
                    arg_index = (arg_index + 1).min(2);
                }
            }
            if cs > 0 {
                let actual = if ck == 1 {
                    crc32fast::hash(&args_out.concat()).to_be_bytes()
                } else if ck == 3 {
                    crc32c::crc32c(&args_out.concat()).to_be_bytes()
                } else {
                    [0; 4]
                };
                if expected != actual {
                    bail!("TChannel response checksum mismatch")
                }
            }
            if has_more == 0 {
                break;
            }
        }
        let response_headers: Option<HashMap<String, String>> =
            serde_json::from_slice(&args_out[1])?;
        let body: Value = serde_json::from_slice(&args_out[2])
            .map_err(|e| anyhow!("malformed JSON response: {e}"))?;
        if app {
            return Err(anyhow!("APPLICATION:{body}"));
        }
        Ok(JsonResponse {
            headers: response_headers,
            body,
            application_error: false,
        })
    };
    match timeout(deadline, work).await {
        Err(_) => Err(JsonCallError::Transport(anyhow!(
            "TChannel JSON call deadline exceeded"
        ))),
        Ok(Err(e)) if e.to_string().starts_with("APPLICATION:") => Err(JsonCallError::Application(
            serde_json::from_str(e.to_string().trim_start_matches("APPLICATION:"))
                .unwrap_or(Value::Null),
        )),
        Ok(Err(e)) => Err(JsonCallError::Transport(e)),
        Ok(Ok(v)) => Ok(v),
    }
}

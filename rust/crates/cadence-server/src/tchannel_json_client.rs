//! One-shot outbound TChannel v2 JSON calls, compatible with tchannel-go v1.34.4.
//! A call owns its TCP connection; dropping its future closes the socket. TChannel CANCEL
//! frames are not sent, so remote cancellation depends on peer disconnect handling.
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
struct ApplicationFailure(Value);
impl std::fmt::Display for ApplicationFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TChannel application failure: {}", self.0)
    }
}
impl std::error::Error for ApplicationFailure {}
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
    let x = take(b, p, 2, "TChannel field")?;
    Ok(u16::from_be_bytes([x[0], x[1]]) as usize)
}
fn read_string(b: &[u8], p: &mut usize) -> Result<String> {
    let n = get16(b, p)?;
    let x = take(b, p, n, "TChannel string")?;
    Ok(std::str::from_utf8(x)?.to_string())
}

fn read_string8(b: &[u8], p: &mut usize) -> Result<String> {
    let n = *b
        .get(*p)
        .ok_or_else(|| anyhow!("truncated TChannel string"))? as usize;
    *p += 1;
    let x = take(b, p, n, "TChannel string")?;
    Ok(std::str::from_utf8(x)?.to_string())
}
fn take<'a>(b: &'a [u8], p: &mut usize, n: usize, what: &str) -> Result<&'a [u8]> {
    let end = p
        .checked_add(n)
        .ok_or_else(|| anyhow!("{what} length overflow"))?;
    let value = b.get(*p..end).ok_or_else(|| anyhow!("truncated {what}"))?;
    *p = end;
    Ok(value)
}
fn byte(b: &[u8], p: &mut usize, what: &str) -> Result<u8> {
    Ok(take(b, p, 1, what)?[0])
}

fn parse_response_frames(frames: Vec<(u8, u32, Vec<u8>)>) -> Result<(bool, [Vec<u8>; 3])> {
    let (mut app, mut arg_index, mut args_out) =
        (false, 0usize, [Vec::new(), Vec::new(), Vec::new()]);
    let mut accumulated = Vec::new();
    let mut checksum_type = None;
    for (frame_number, (kind, id, p)) in frames.into_iter().enumerate() {
        if id != 2 {
            bail!("unexpected TChannel response id {id}")
        }
        if (frame_number == 0 && kind != 4) || (frame_number > 0 && kind != 0x14) {
            bail!("unexpected TChannel response frame {kind:#x}")
        }
        let mut q = 0;
        let flags = byte(&p, &mut q, "response flags")?;
        if flags & !1 != 0 {
            bail!("unsupported TChannel response flags")
        }
        if kind == 4 {
            app = byte(&p, &mut q, "response code")? != 0;
            take(&p, &mut q, 25, "response tracing")?;
            let count = byte(&p, &mut q, "response header count")? as usize;
            for _ in 0..count {
                read_string8(&p, &mut q)?;
                read_string8(&p, &mut q)?;
            }
        }
        let ck = byte(&p, &mut q, "checksum type")?;
        if !matches!(ck, 0 | 1 | 3) {
            bail!("unsupported TChannel checksum type {ck}")
        }
        if checksum_type.replace(ck).is_some_and(|old| old != ck) {
            bail!("TChannel checksum type changed between fragments")
        }
        let cs = if ck == 0 { 0 } else { 4 };
        let expected = take(&p, &mut q, cs, "checksum")?.to_vec();
        let mut last_chunk_terminated = false;
        while q < p.len() {
            if arg_index >= 3 {
                bail!("too many TChannel response arguments")
            }
            let n = get16(&p, &mut q)?;
            let bytes = take(&p, &mut q, n, "response argument")?;
            args_out[arg_index].extend_from_slice(bytes);
            accumulated.extend_from_slice(bytes);
            // In-frame chunk transitions end an argument; a zero-length chunk
            // at a fragment boundary marks the transition across frames.
            last_chunk_terminated = n == 0 || q < p.len();
            if last_chunk_terminated {
                arg_index += 1;
            }
        }
        if cs > 0 {
            let actual = if ck == 1 {
                crc32fast::hash(&accumulated).to_be_bytes()
            } else {
                crc32c::crc32c(&accumulated).to_be_bytes()
            };
            if expected != actual {
                bail!("TChannel response checksum mismatch")
            }
        }
        if flags & 1 == 0 {
            if !last_chunk_terminated && arg_index < 3 {
                arg_index += 1;
            }
            if arg_index != 3 {
                bail!("TChannel response ended after {arg_index} arguments")
            }
            return Ok((app, args_out));
        }
    }
    bail!("TChannel response missing final fragment")
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
    let started_at = tokio::time::Instant::now();
    let work = async {
        let mut s = TcpStream::connect(addr).await?;
        s.set_nodelay(true)?;
        let mut init = 2u16.to_be_bytes().to_vec();
        init.extend(headers16(&[
            ("host_port", "0.0.0.0:0"),
            ("process_name", "cadence-rust-ringpop-client"),
        ])?);
        write_frame(&mut s, 1, 1, &init).await?;
        let (k, init_id, p) = read_frame(&mut s).await?;
        if k != 2 || init_id != 1 || p.len() < 4 {
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
        let remaining = deadline.saturating_sub(started_at.elapsed());
        let mut head = vec![1];
        head.extend((remaining.as_millis().min(u32::MAX as u128) as u32).to_be_bytes());
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
        let mut response_frames = Vec::new();
        loop {
            let (k, id, p) = read_frame(&mut s).await?;
            if k == 0xff {
                if id != 2 {
                    bail!("unexpected TChannel error id {id}")
                }
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
            let more = p.first().copied().unwrap_or(0);
            response_frames.push((k, id, p));
            if more & 1 == 0 {
                break;
            }
        }
        let (app, args_out) = parse_response_frames(response_frames)?;
        let response_headers: Option<HashMap<String, String>> =
            serde_json::from_slice(&args_out[1])?;
        let body: Value = serde_json::from_slice(&args_out[2])
            .map_err(|e| anyhow!("malformed JSON response: {e}"))?;
        if app {
            return Err(anyhow::Error::new(ApplicationFailure(body)));
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
        Ok(Err(e)) if e.downcast_ref::<ApplicationFailure>().is_some() => Err(
            JsonCallError::Application(e.downcast::<ApplicationFailure>().unwrap().0),
        ),
        Ok(Err(e)) => Err(JsonCallError::Transport(e)),
        Ok(Ok(v)) => Ok(v),
    }
}

#[cfg(test)]
mod adversarial_tests {
    use super::*;
    use tokio::net::TcpListener;

    async fn mock_call(payload: Vec<u8>, kind: u8, id: u32) -> JsonCallError {
        mock_frames(vec![(kind, id, payload)]).await.unwrap_err()
    }

    async fn mock_frames(
        frames: Vec<(u8, u32, Vec<u8>)>,
    ) -> std::result::Result<JsonResponse, JsonCallError> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            read_frame(&mut s).await.unwrap();
            write_frame(&mut s, 2, 1, &[0, 2, 0, 0]).await.unwrap();
            read_frame(&mut s).await.unwrap();
            for (kind, id, payload) in frames {
                write_frame(&mut s, kind, id, &payload).await.unwrap();
            }
        });
        let result = call(
            &addr.to_string(),
            "svc",
            "method",
            None,
            &Value::Null,
            Duration::from_secs(2),
        )
        .await;
        peer.await.unwrap();
        result
    }

    fn valid_response() -> Vec<u8> {
        let mut p = vec![0, 0];
        p.extend([0; 25]);
        p.extend([0, 0]); // no transport headers, no checksum
        for arg in [b"method".as_slice(), b"{}\n", b"{}\n"] {
            p.extend((arg.len() as u16).to_be_bytes());
            p.extend(arg);
        }
        p
    }

    #[tokio::test]
    async fn every_truncated_call_res_fixed_header_is_an_error() {
        for n in 0..29 {
            let err = mock_call(vec![0; n], 4, 2).await;
            assert!(
                matches!(err, JsonCallError::Transport(_)),
                "prefix length {n}: {err}"
            );
        }
    }

    #[tokio::test]
    async fn malformed_ids_types_boundaries_and_checksums_are_rejected() {
        assert!(matches!(
            mock_call(valid_response(), 4, 9).await,
            JsonCallError::Transport(_)
        ));
        assert!(matches!(
            mock_call(valid_response(), 0x14, 2).await,
            JsonCallError::Transport(_)
        ));
        let mut bad_flags = valid_response();
        bad_flags[0] = 2;
        assert!(matches!(
            mock_call(bad_flags, 4, 2).await,
            JsonCallError::Transport(_)
        ));
        let mut more = valid_response();
        more[0] = 1;
        assert!(matches!(
            mock_frames(vec![(4, 2, more), (4, 2, valid_response())]).await,
            Err(JsonCallError::Transport(_))
        ));
        let mut extra = valid_response();
        extra.extend([0, 0]);
        assert!(matches!(
            mock_call(extra, 4, 2).await,
            JsonCallError::Transport(_)
        ));
        let mut bad_checksum = valid_response();
        bad_checksum[28] = 1; // IEEE checksum type, missing bytes
        assert!(matches!(
            mock_call(bad_checksum, 4, 2).await,
            JsonCallError::Transport(_)
        ));
        let mut bad_payload = valid_response();
        bad_payload[30] = 0xff; // first arg length exceeds remaining payload
        assert!(matches!(
            mock_call(bad_payload, 4, 2).await,
            JsonCallError::Transport(_)
        ));
        let mut bad_header_count = vec![0, 0];
        bad_header_count.extend([0; 25]);
        bad_header_count.extend([1, 0]); // one header declared, no header bytes
        assert!(matches!(
            mock_call(bad_header_count, 4, 2).await,
            JsonCallError::Transport(_)
        ));
        let mut corrupt_crc = valid_response();
        corrupt_crc[28] = 1;
        corrupt_crc.splice(29..29, [0, 0, 0, 0]);
        assert!(matches!(
            mock_call(corrupt_crc, 4, 2).await,
            JsonCallError::Transport(_)
        ));
    }

    #[tokio::test]
    async fn call_ttl_subtracts_connect_and_init_elapsed_time() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            read_frame(&mut s).await.unwrap();
            tokio::time::sleep(Duration::from_millis(80)).await;
            write_frame(&mut s, 2, 1, &[0, 2, 0, 0]).await.unwrap();
            let (_, _, call_payload) = read_frame(&mut s).await.unwrap();
            let ttl = u32::from_be_bytes(call_payload[1..5].try_into().unwrap());
            write_frame(&mut s, 4, 2, &[0]).await.unwrap();
            ttl
        });
        let result = call(
            &addr.to_string(),
            "svc",
            "method",
            None,
            &Value::Null,
            Duration::from_millis(500),
        )
        .await;
        assert!(matches!(result, Err(JsonCallError::Transport(_))));
        let ttl = peer.await.unwrap();
        assert!(
            ttl < 500,
            "wire TTL should account for INIT delay, got {ttl}ms"
        );
        assert!(
            ttl >= 300,
            "wire TTL unexpectedly lost more than INIT delay: {ttl}ms"
        );
    }

    #[tokio::test]
    async fn accumulated_crc32_and_crc32c_accept_multiple_tcp_fragments() {
        for checksum in [1u8, 3] {
            let bytes = b"method{}\n{}\n";
            let first_sum = if checksum == 1 {
                crc32fast::hash(b"method")
            } else {
                crc32c::crc32c(b"method")
            };
            let sum = if checksum == 1 {
                crc32fast::hash(bytes)
            } else {
                crc32c::crc32c(bytes)
            };
            let mut first = vec![1, 0];
            first.extend([0; 25]);
            first.extend([0, checksum]);
            first.extend(first_sum.to_be_bytes());
            first.extend(6u16.to_be_bytes());
            first.extend(b"method");
            let mut second = vec![0, checksum];
            second.extend(sum.to_be_bytes());
            second.extend(0u16.to_be_bytes()); // argument transition across fragments
            second.extend(3u16.to_be_bytes());
            second.extend(b"{}\n");
            second.extend(3u16.to_be_bytes());
            second.extend(b"{}\n");
            let response = mock_frames(vec![(4, 2, first), (0x14, 2, second)])
                .await
                .unwrap();
            assert_eq!(response.body, serde_json::json!({}));
        }
    }
}

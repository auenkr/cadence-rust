//! HTTP object stores behind the Go-compatible s3store / gstorage archivers:
//! S3 (SigV4, path/virtual-hosted style, endpoint override) and GCS (JSON API,
//! service-account / emulator auth). Every request runs on a dedicated std
//! thread so blocking reqwest is safe inside the tokio runtime.

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cadence_common::cloud_archiver::{
    CloudStoreFactory, GcsConfig, ListPage, ObjectStore, S3Config, StoreError, StoreErrorKind,
};
use sha2::{Digest, Sha256};

type Raw = (u16, Vec<u8>);

/// Runs `f` on its own thread (blocking reqwest builds a private runtime).
fn on_thread<T: Send>(f: impl FnOnce() -> Result<T, StoreError> + Send) -> Result<T, StoreError> {
    std::thread::scope(|s| {
        s.spawn(f).join().unwrap_or_else(|_| {
            Err(StoreError::new(
                StoreErrorKind::Other,
                "store request panicked",
            ))
        })
    })
}

fn client() -> Result<reqwest::blocking::Client, StoreError> {
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .map_err(|e| StoreError::new(StoreErrorKind::Other, e.to_string()))
}

fn transport(e: reqwest::Error) -> StoreError {
    StoreError::new(StoreErrorKind::Retryable, e.to_string())
}

fn send(req: reqwest::blocking::RequestBuilder) -> Result<Raw, StoreError> {
    let resp = req.send().map_err(transport)?;
    let status = resp.status().as_u16();
    let body = resp.bytes().map_err(transport)?.to_vec();
    Ok((status, body))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn sha256_hex(data: &[u8]) -> String {
    hex(&Sha256::digest(data))
}

pub fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut k = if key.len() > 64 {
        Sha256::digest(key).to_vec()
    } else {
        key.to_vec()
    };
    k.resize(64, 0);
    let mut inner = Sha256::new();
    inner.update(k.iter().map(|b| b ^ 0x36).collect::<Vec<_>>());
    inner.update(data);
    let mut outer = Sha256::new();
    outer.update(k.iter().map(|b| b ^ 0x5c).collect::<Vec<_>>());
    outer.update(inner.finalize());
    outer.finalize().to_vec()
}

/// RFC 3986 encoding as required by SigV4 (`/` kept when `keep_slash`).
fn uri_encode(s: &str, keep_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b'/' if keep_slash => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn xml_unescape(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

/// First `<tag>value</tag>` inside `xml`.
fn xml_tag<'a>(xml: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&format!("</{tag}>"))? + start;
    Some(&xml[start..end])
}

/// Inner text of every `<block>...</block>` element.
fn xml_blocks<'a>(xml: &'a str, block: &str) -> Vec<&'a str> {
    let (open, close) = (format!("<{block}>"), format!("</{block}>"));
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(i) = rest.find(&open) {
        let after = &rest[i + open.len()..];
        let Some(j) = after.find(&close) else { break };
        out.push(&after[..j]);
        rest = &after[j + close.len()..];
    }
    out
}

// ---------------------------------------------------------------------- S3

#[derive(Clone)]
struct AwsCreds {
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
}

fn env_creds() -> Option<AwsCreds> {
    let access_key = std::env::var("AWS_ACCESS_KEY_ID")
        .or_else(|_| std::env::var("AWS_ACCESS_KEY"))
        .ok()?;
    let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY")
        .or_else(|_| std::env::var("AWS_SECRET_KEY"))
        .ok()?;
    Some(AwsCreds {
        access_key,
        secret_key,
        session_token: std::env::var("AWS_SESSION_TOKEN")
            .ok()
            .filter(|t| !t.is_empty()),
    })
}

pub struct S3Store {
    region: String,
    endpoint: Option<String>,
    force_path_style: bool,
    creds: Option<AwsCreds>,
}

struct Target {
    scheme: String,
    host: String,
    path: String,
}

/// UTC `(YYYYMMDD, YYYYMMDD'T'HHMMSS'Z')`.
fn amz_now() -> (String, String) {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let dt =
        chrono::DateTime::from_timestamp(i64::try_from(secs).unwrap_or(0), 0).unwrap_or_default();
    (
        dt.format("%Y%m%d").to_string(),
        dt.format("%Y%m%dT%H%M%SZ").to_string(),
    )
}

impl S3Store {
    pub fn new(config: &S3Config) -> Self {
        Self::with_creds(config, env_creds())
    }

    pub fn with_creds_for_test(config: &S3Config, key: &str, secret: &str) -> Self {
        Self::with_creds(
            config,
            Some(AwsCreds {
                access_key: key.to_owned(),
                secret_key: secret.to_owned(),
                session_token: None,
            }),
        )
    }

    fn with_creds(config: &S3Config, creds: Option<AwsCreds>) -> Self {
        Self {
            region: if config.region.is_empty() {
                "us-east-1".to_owned()
            } else {
                config.region.clone()
            },
            endpoint: config
                .endpoint
                .clone()
                .filter(|e| !e.is_empty())
                .map(|e| e.trim_end_matches('/').to_owned()),
            force_path_style: config.force_path_style,
            creds,
        }
    }

    fn target(&self, bucket: &str, key: &str) -> Target {
        let (scheme, authority) = match &self.endpoint {
            Some(ep) => match ep.split_once("://") {
                Some((s, a)) => (s.to_owned(), Some(a.to_owned())),
                None => ("https".to_owned(), Some(ep.clone())),
            },
            None => ("https".to_owned(), None),
        };
        if let Some(arn) = bucket.strip_prefix("arn:") {
            // arn:<partition>:s3:<region>:<account>:accesspoint/<name>
            let parts: Vec<&str> = arn.split(':').collect();
            if parts.len() >= 5 {
                let name = parts[4..].join(":");
                let name = name.strip_prefix("accesspoint/").unwrap_or(&name);
                let dns = if parts[0] == "aws-cn" {
                    "amazonaws.com.cn"
                } else {
                    "amazonaws.com"
                };
                return Target {
                    scheme,
                    host: authority.unwrap_or_else(|| {
                        format!("{name}-{}.s3-accesspoint.{}.{dns}", parts[3], parts[2])
                    }),
                    path: format!("/{key}"),
                };
            }
        }
        match authority {
            Some(a) if self.force_path_style => Target {
                scheme,
                host: a,
                path: if key.is_empty() {
                    format!("/{bucket}")
                } else {
                    format!("/{bucket}/{key}")
                },
            },
            Some(a) => Target {
                scheme,
                host: format!("{bucket}.{a}"),
                path: format!("/{key}"),
            },
            None if self.force_path_style => Target {
                scheme,
                host: format!("s3.{}.amazonaws.com", self.region),
                path: if key.is_empty() {
                    format!("/{bucket}")
                } else {
                    format!("/{bucket}/{key}")
                },
            },
            None => Target {
                scheme,
                host: format!("{bucket}.s3.{}.amazonaws.com", self.region),
                path: format!("/{key}"),
            },
        }
    }

    /// Computes the SigV4 `Authorization` header and the signed extra headers.
    fn sign(
        &self,
        method: &str,
        host: &str,
        path: &str,
        query: &[(String, String)],
        now: &(String, String),
    ) -> Vec<(String, String)> {
        let payload_hash = sha256_hex(b"");
        let mut headers = vec![
            ("host".to_owned(), host.to_owned()),
            ("x-amz-content-sha256".to_owned(), payload_hash.clone()),
            ("x-amz-date".to_owned(), now.1.clone()),
        ];
        let Some(creds) = &self.creds else {
            return Vec::new();
        };
        if let Some(t) = &creds.session_token {
            headers.push(("x-amz-security-token".to_owned(), t.clone()));
        }
        headers.sort();
        let mut q: Vec<(String, String)> = query
            .iter()
            .map(|(k, v)| (uri_encode(k, false), uri_encode(v, false)))
            .collect();
        q.sort();
        let canonical_query = q
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        let canonical_headers: String = headers
            .iter()
            .map(|(k, v)| format!("{k}:{}\n", v.trim()))
            .collect();
        let signed = headers
            .iter()
            .map(|(k, _)| k.as_str())
            .collect::<Vec<_>>()
            .join(";");
        let canonical = format!(
            "{method}\n{}\n{canonical_query}\n{canonical_headers}\n{signed}\n{payload_hash}",
            uri_encode(path, true)
        );
        let scope = format!("{}/{}/s3/aws4_request", now.0, self.region);
        let to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{scope}\n{}",
            now.1,
            sha256_hex(canonical.as_bytes())
        );
        let k_date = hmac_sha256(
            format!("AWS4{}", creds.secret_key).as_bytes(),
            now.0.as_bytes(),
        );
        let k_region = hmac_sha256(&k_date, self.region.as_bytes());
        let k_service = hmac_sha256(&k_region, b"s3");
        let k_signing = hmac_sha256(&k_service, b"aws4_request");
        let signature = hex(&hmac_sha256(&k_signing, to_sign.as_bytes()));
        let mut out: Vec<(String, String)> =
            headers.into_iter().filter(|(k, _)| k != "host").collect();
        out.push((
            "authorization".to_owned(),
            format!(
                "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed}, Signature={signature}",
                creds.access_key
            ),
        ));
        out
    }

    fn request(
        &self,
        method: &str,
        bucket: &str,
        key: &str,
        query: Vec<(String, String)>,
    ) -> Result<Raw, StoreError> {
        let t = self.target(bucket, key);
        let now = amz_now();
        let headers = self.sign(method, &t.host, &t.path, &query, &now);
        let mut url = format!("{}://{}{}", t.scheme, t.host, uri_encode(&t.path, true));
        if !query.is_empty() {
            let qs: Vec<String> = query
                .iter()
                .map(|(k, v)| format!("{}={}", uri_encode(k, false), uri_encode(v, false)))
                .collect();
            url = format!("{url}?{}", qs.join("&"));
        }
        on_thread(move || {
            let c = client()?;
            let m = reqwest::Method::from_bytes(method.as_bytes())
                .map_err(|e| StoreError::new(StoreErrorKind::Other, e.to_string()))?;
            let mut req = c.request(m, &url);
            for (k, v) in &headers {
                req = req.header(k.as_str(), v.as_str());
            }
            send(req)
        })
    }

    fn error(status: u16, body: &[u8]) -> StoreError {
        let text = String::from_utf8_lossy(body);
        let code = xml_tag(&text, "Code").unwrap_or_default();
        let message = xml_tag(&text, "Message").map(xml_unescape);
        let kind = match code {
            "NoSuchBucket" => StoreErrorKind::NoSuchBucket,
            "NoSuchKey" => StoreErrorKind::NoSuchKey,
            "SlowDown"
            | "Throttling"
            | "ThrottlingException"
            | "RequestTimeout"
            | "InternalError"
            | "ServiceUnavailable" => StoreErrorKind::Retryable,
            "" if status == 404 => StoreErrorKind::NotFound,
            _ if status >= 500 => StoreErrorKind::Retryable,
            _ => StoreErrorKind::Other,
        };
        let msg = match (code, message) {
            ("", _) => format!("http status {status}"),
            (c, Some(m)) => format!("{c}: {m}"),
            (c, None) => c.to_owned(),
        };
        StoreError::new(kind, msg)
    }
}

impl ObjectStore for S3Store {
    fn head_bucket(&self, bucket: &str) -> Result<(), StoreError> {
        let (status, body) = self.request("HEAD", bucket, "", Vec::new())?;
        if (200..300).contains(&status) {
            Ok(())
        } else {
            Err(Self::error(status, &body))
        }
    }

    fn get_object(&self, bucket: &str, key: &str) -> Result<Vec<u8>, StoreError> {
        let (status, body) = self.request("GET", bucket, key, Vec::new())?;
        if (200..300).contains(&status) {
            Ok(body)
        } else {
            Err(Self::error(status, &body))
        }
    }

    fn list(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        max_keys: Option<usize>,
        token: Option<&str>,
    ) -> Result<ListPage, StoreError> {
        let mut q = vec![
            ("list-type".to_owned(), "2".to_owned()),
            ("prefix".to_owned(), prefix.to_owned()),
        ];
        if let Some(d) = delimiter {
            q.push(("delimiter".to_owned(), d.to_owned()));
        }
        if let Some(m) = max_keys {
            q.push(("max-keys".to_owned(), m.to_string()));
        }
        if let Some(t) = token {
            q.push(("continuation-token".to_owned(), t.to_owned()));
        }
        let (status, body) = self.request("GET", bucket, "", q)?;
        if !(200..300).contains(&status) {
            return Err(Self::error(status, &body));
        }
        let xml = String::from_utf8_lossy(&body);
        let keys = xml_blocks(&xml, "Contents")
            .into_iter()
            .filter_map(|c| xml_tag(c, "Key").map(xml_unescape))
            .collect();
        let common_prefixes: Vec<String> = xml_blocks(&xml, "CommonPrefixes")
            .into_iter()
            .filter_map(|c| xml_tag(c, "Prefix").map(xml_unescape))
            .collect();
        let truncated = xml_tag(&xml, "IsTruncated") == Some("true");
        let next_token = xml_tag(&xml, "NextContinuationToken")
            .map(xml_unescape)
            .filter(|_| truncated);
        Ok(ListPage {
            keys,
            common_prefixes,
            next_token,
        })
    }
}

// --------------------------------------------------------------------- GCS

#[derive(serde::Deserialize, Clone)]
struct ServiceAccount {
    client_email: String,
    private_key: String,
    #[serde(default)]
    token_uri: String,
}

enum GcsAuth {
    None,
    ServiceAccount(ServiceAccount),
}

pub struct GcsStore {
    base: String,
    auth: GcsAuth,
    token: Mutex<Option<(String, u64)>>,
}

const GCS_SCOPE: &str = "https://www.googleapis.com/auth/devstorage.read_write";

impl GcsStore {
    /// Go `connector.NewClient`: emulator via `STORAGE_EMULATOR_HOST`, else
    /// service-account credentials (config path or ADC env var).
    pub fn new(config: &GcsConfig) -> Result<Self, String> {
        let emulator = std::env::var("STORAGE_EMULATOR_HOST")
            .ok()
            .filter(|h| !h.is_empty());
        let cred_path = if config.credentials_path.is_empty() {
            std::env::var("GOOGLE_APPLICATION_CREDENTIALS").unwrap_or_default()
        } else {
            config.credentials_path.clone()
        };
        if let Some(host) = emulator {
            let base = if host.contains("://") {
                host
            } else {
                format!("http://{host}")
            };
            return Ok(Self::with_base(base.trim_end_matches('/').to_owned(), None));
        }
        let auth = if cred_path.is_empty() {
            None
        } else {
            let raw = std::fs::read_to_string(&cred_path).map_err(|e| e.to_string())?;
            Some(serde_json::from_str::<ServiceAccount>(&raw).map_err(|e| e.to_string())?)
        };
        Ok(Self::with_base(
            "https://storage.googleapis.com".to_owned(),
            auth,
        ))
    }

    fn with_base(base: String, sa: Option<ServiceAccount>) -> Self {
        Self {
            base,
            auth: sa.map_or(GcsAuth::None, GcsAuth::ServiceAccount),
            token: Mutex::new(None),
        }
    }

    pub fn for_test(base: &str) -> Self {
        Self::with_base(base.to_owned(), None)
    }

    fn bearer(&self) -> Result<Option<String>, StoreError> {
        let GcsAuth::ServiceAccount(sa) = &self.auth else {
            return Ok(None);
        };
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        if let Ok(guard) = self.token.lock() {
            if let Some((t, exp)) = guard.as_ref() {
                if *exp > now + 60 {
                    return Ok(Some(t.clone()));
                }
            }
        }
        let token_uri = if sa.token_uri.is_empty() {
            "https://oauth2.googleapis.com/token".to_owned()
        } else {
            sa.token_uri.clone()
        };
        let claims = serde_json::json!({
            "iss": sa.client_email, "scope": GCS_SCOPE, "aud": token_uri,
            "iat": now, "exp": now + 3600,
        });
        let key = jsonwebtoken::EncodingKey::from_rsa_pem(sa.private_key.as_bytes())
            .map_err(|e| StoreError::new(StoreErrorKind::Other, e.to_string()))?;
        let jwt = jsonwebtoken::encode(
            &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
            &claims,
            &key,
        )
        .map_err(|e| StoreError::new(StoreErrorKind::Other, e.to_string()))?;
        let (status, body) = on_thread(|| {
            send(client()?.post(&token_uri).form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                ("assertion", jwt.as_str()),
            ]))
        })?;
        if status != 200 {
            return Err(StoreError::new(
                StoreErrorKind::Other,
                format!("oauth2: cannot fetch token: {status}"),
            ));
        }
        let v: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|e| StoreError::new(StoreErrorKind::Other, e.to_string()))?;
        let token = v["access_token"].as_str().unwrap_or_default().to_owned();
        let expires = v["expires_in"].as_u64().unwrap_or(3600);
        if let Ok(mut guard) = self.token.lock() {
            *guard = Some((token.clone(), now + expires));
        }
        Ok(Some(token))
    }

    fn get(&self, url: String) -> Result<Raw, StoreError> {
        let bearer = self.bearer()?;
        on_thread(move || {
            let mut req = client()?.get(&url);
            if let Some(t) = bearer {
                req = req.bearer_auth(t);
            }
            send(req)
        })
    }

    fn error(status: u16, body: &[u8], not_found: StoreErrorKind) -> StoreError {
        let kind = match status {
            404 => not_found,
            429 | 500..=599 => StoreErrorKind::Retryable,
            _ => StoreErrorKind::Other,
        };
        let msg = serde_json::from_slice::<serde_json::Value>(body)
            .ok()
            .and_then(|v| v["error"]["message"].as_str().map(str::to_owned))
            .unwrap_or_else(|| format!("http status {status}"));
        StoreError::new(kind, msg)
    }
}

impl ObjectStore for GcsStore {
    fn head_bucket(&self, bucket: &str) -> Result<(), StoreError> {
        let url = format!("{}/storage/v1/b/{}", self.base, uri_encode(bucket, false));
        let (status, body) = self.get(url)?;
        if status == 200 {
            Ok(())
        } else {
            Err(Self::error(status, &body, StoreErrorKind::NoSuchBucket))
        }
    }

    fn get_object(&self, bucket: &str, key: &str) -> Result<Vec<u8>, StoreError> {
        let url = format!(
            "{}/storage/v1/b/{}/o/{}?alt=media",
            self.base,
            uri_encode(bucket, false),
            uri_encode(key, false)
        );
        let (status, body) = self.get(url)?;
        if status == 200 {
            Ok(body)
        } else {
            Err(Self::error(status, &body, StoreErrorKind::NoSuchKey))
        }
    }

    fn list(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        max_keys: Option<usize>,
        token: Option<&str>,
    ) -> Result<ListPage, StoreError> {
        let mut url = format!(
            "{}/storage/v1/b/{}/o?prefix={}",
            self.base,
            uri_encode(bucket, false),
            uri_encode(prefix, false)
        );
        if let Some(d) = delimiter {
            url.push_str(&format!("&delimiter={}", uri_encode(d, false)));
        }
        if let Some(m) = max_keys {
            url.push_str(&format!("&maxResults={m}"));
        }
        if let Some(t) = token {
            url.push_str(&format!("&pageToken={}", uri_encode(t, false)));
        }
        let (status, body) = self.get(url)?;
        if status != 200 {
            return Err(Self::error(status, &body, StoreErrorKind::NoSuchBucket));
        }
        let v: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|e| StoreError::new(StoreErrorKind::Other, e.to_string()))?;
        let strings = |field: &str, inner: Option<&str>| -> Vec<String> {
            v[field]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|i| match inner {
                            Some(k) => i[k].as_str(),
                            None => i.as_str(),
                        })
                        .map(str::to_owned)
                        .collect()
                })
                .unwrap_or_default()
        };
        Ok(ListPage {
            keys: strings("items", Some("name")),
            common_prefixes: strings("prefixes", None),
            next_token: v["nextPageToken"].as_str().map(str::to_owned),
        })
    }
}

/// Production factory wired into the Frontend archiver provider.
pub struct HttpStoreFactory;

impl CloudStoreFactory for HttpStoreFactory {
    fn s3(&self, config: &S3Config) -> Result<Arc<dyn ObjectStore>, String> {
        Ok(Arc::new(S3Store::new(config)))
    }

    fn gcs(&self, config: &GcsConfig) -> Result<Arc<dyn ObjectStore>, String> {
        Ok(Arc::new(GcsStore::new(config)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    /// One-shot-per-response fake HTTP server; returns the raw requests seen.
    fn serve(responses: Vec<(u16, String)>) -> (String, std::thread::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = format!("http://{}", listener.local_addr().unwrap());
        let handle = std::thread::spawn(move || {
            let mut seen = Vec::new();
            for (status, body) in responses {
                let (mut s, _) = listener.accept().unwrap();
                let mut buf = [0_u8; 8192];
                let n = s.read(&mut buf).unwrap();
                seen.push(String::from_utf8_lossy(&buf[..n]).into_owned());
                let reply = format!(
                    "HTTP/1.1 {status} X\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                s.write_all(reply.as_bytes()).unwrap();
            }
            seen
        });
        (addr, handle)
    }

    #[test]
    fn hmac_matches_rfc4231_vector() {
        assert_eq!(
            hex(&hmac_sha256(&[0x0b; 20], b"Hi There")),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
    }

    #[test]
    fn s3_path_style_list_get_head_and_errors() {
        let list = "<ListBucketResult><IsTruncated>true</IsTruncated>\
            <Contents><Key>a/b&amp;c</Key></Contents><Contents><Key>a/d</Key></Contents>\
            <CommonPrefixes><Prefix>a/1/</Prefix></CommonPrefixes>\
            <NextContinuationToken>tok1</NextContinuationToken></ListBucketResult>";
        let nokey = "<Error><Code>NoSuchKey</Code><Message>nope</Message></Error>";
        let (addr, handle) = serve(vec![
            (200, list.to_owned()),
            (200, "payload".to_owned()),
            (404, nokey.to_owned()),
            (404, String::new()),
        ]);
        let store = S3Store::with_creds_for_test(
            &S3Config {
                region: "us-east-1".into(),
                endpoint: Some(addr),
                force_path_style: true,
            },
            "AKID",
            "secret",
        );
        let page = store.list("bkt", "a/", Some("/"), Some(2), None).unwrap();
        assert_eq!(page.keys, ["a/b&c", "a/d"]);
        assert_eq!(page.common_prefixes, ["a/1/"]);
        assert_eq!(page.next_token.as_deref(), Some("tok1"));
        assert_eq!(store.get_object("bkt", "a/d").unwrap(), b"payload");
        assert_eq!(
            store.get_object("bkt", "missing").unwrap_err().kind,
            StoreErrorKind::NoSuchKey
        );
        assert_eq!(
            store.head_bucket("bkt").unwrap_err().kind,
            StoreErrorKind::NotFound
        );
        let seen = handle.join().unwrap();
        assert!(
            seen[0]
                .starts_with("GET /bkt?delimiter=%2F&list-type=2&max-keys=2&prefix=a%2F HTTP/1.1")
                || seen[0].starts_with(
                    "GET /bkt?list-type=2&prefix=a%2F&delimiter=%2F&max-keys=2 HTTP/1.1"
                ),
            "{}",
            seen[0]
        );
        let lower = seen[0].to_lowercase();
        assert!(lower.contains("authorization: aws4-hmac-sha256 credential=akid/"));
        assert!(lower.contains("signedheaders=host;x-amz-content-sha256;x-amz-date"));
        assert!(seen[3].starts_with("HEAD /bkt "), "{}", seen[3]);
    }

    #[test]
    fn s3_virtual_hosted_arn_and_endpoint_targets() {
        let virtual_host = S3Store::with_creds_for_test(
            &S3Config {
                region: "eu-west-1".into(),
                endpoint: None,
                force_path_style: false,
            },
            "k",
            "s",
        );
        let t = virtual_host.target("bkt", "p/k");
        assert_eq!(
            (t.host.as_str(), t.path.as_str()),
            ("bkt.s3.eu-west-1.amazonaws.com", "/p/k")
        );
        let t = virtual_host.target("arn:aws:s3:eu-west-1:123456789012:accesspoint/ap", "k");
        assert_eq!(
            t.host,
            "ap-123456789012.s3-accesspoint.eu-west-1.amazonaws.com"
        );
        let custom = S3Store::with_creds_for_test(
            &S3Config {
                region: "r".into(),
                endpoint: Some("http://minio:9000/".into()),
                force_path_style: false,
            },
            "k",
            "s",
        );
        let t = custom.target("bkt", "k");
        assert_eq!(
            (t.scheme.as_str(), t.host.as_str()),
            ("http", "bkt.minio:9000")
        );
    }

    #[test]
    fn gcs_list_get_and_not_found() {
        let list = r#"{"items":[{"name":"p/a"},{"name":"p/b"}],"nextPageToken":"n2"}"#;
        let (addr, handle) = serve(vec![
            (200, "{}".to_owned()),
            (200, list.to_owned()),
            (200, "blob".to_owned()),
            (404, r#"{"error":{"message":"No such object"}}"#.to_owned()),
            (404, "{}".to_owned()),
        ]);
        let store = GcsStore::for_test(&addr);
        store.head_bucket("bkt").unwrap();
        let page = store.list("bkt", "p/", None, Some(2), Some("n1")).unwrap();
        assert_eq!(page.keys, ["p/a", "p/b"]);
        assert_eq!(page.next_token.as_deref(), Some("n2"));
        assert_eq!(store.get_object("bkt", "p/a").unwrap(), b"blob");
        let e = store.get_object("bkt", "gone").unwrap_err();
        assert_eq!(
            (e.kind, e.message.as_str()),
            (StoreErrorKind::NoSuchKey, "No such object")
        );
        assert_eq!(
            store.head_bucket("zzz").unwrap_err().kind,
            StoreErrorKind::NoSuchBucket
        );
        let seen = handle.join().unwrap();
        assert!(seen[0].starts_with("GET /storage/v1/b/bkt HTTP/1.1"));
        assert!(
            seen[1].contains("prefix=p%2F")
                && seen[1].contains("maxResults=2")
                && seen[1].contains("pageToken=n1")
        );
        assert!(seen[2].starts_with("GET /storage/v1/b/bkt/o/p%2Fa?alt=media"));
    }
}

// Copyright (c) 2016 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

pub use self::{read_packet::ReadPacket, write_packet::WritePacket};

use bytes::BytesMut;
use futures_core::{ready, stream};
use mysql_common::proto::codec::PacketCodec as PacketCodecInner;
#[cfg(unix)]
use tokio::io::AsyncWriteExt;
use tokio::{
    io::{AsyncRead, AsyncWrite, ErrorKind::Interrupted, ReadBuf},
    net::TcpStream,
};
use tokio_util::codec::{Decoder, Encoder, Framed, FramedParts};

#[cfg(unix)]
use std::path::Path;
use std::{
    fmt,
    future::Future,
    io::{
        self,
        ErrorKind::{BrokenPipe, NotConnected, Other},
    },
    mem::replace,
    net::SocketAddr,
    ops::{Deref, DerefMut},
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use crate::{
    buffer_pool::PooledBuf,
    error::IoError,
    opts::{HostPortOrUrl, DEFAULT_PORT},
};

#[cfg(unix)]
use crate::io::socket::Socket;

mod tls;

pub(crate) use self::tls::TlsConnector;

macro_rules! with_interrupted {
    ($e:expr) => {
        loop {
            match $e {
                Poll::Ready(Err(err)) if err.kind() == Interrupted => continue,
                x => break x,
            }
        }
    };
}

mod read_packet;
mod socket;
mod write_packet;

#[derive(Debug)]
pub struct PacketCodec {
    inner: PacketCodecInner,
    decode_buf: PooledBuf,
}

impl Default for PacketCodec {
    fn default() -> Self {
        Self {
            inner: Default::default(),
            decode_buf: crate::buffer_pool().get(),
        }
    }
}

impl Deref for PacketCodec {
    type Target = PacketCodecInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for PacketCodec {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Decoder for PacketCodec {
    type Item = PooledBuf;
    type Error = IoError;

    fn decode(&mut self, src: &mut BytesMut) -> std::result::Result<Option<Self::Item>, IoError> {
        if self.inner.decode(src, self.decode_buf.as_mut())? {
            let new_buf = crate::buffer_pool().get();
            Ok(Some(replace(&mut self.decode_buf, new_buf)))
        } else {
            Ok(None)
        }
    }
}

impl Encoder<PooledBuf> for PacketCodec {
    type Error = IoError;

    fn encode(&mut self, item: PooledBuf, dst: &mut BytesMut) -> std::result::Result<(), IoError> {
        Ok(self.inner.encode(&mut item.as_ref(), dst)?)
    }
}

#[derive(Debug)]
pub(crate) enum EndpointTransport {
    Plain(Option<TcpStream>),
    #[cfg(feature = "native-tls-tls")]
    Secure(tokio_native_tls::TlsStream<TlsHandshakeStream<TcpStream>>),
    #[cfg(feature = "rustls-tls")]
    Secure(tokio_rustls::client::TlsStream<TlsHandshakeStream<TcpStream>>),
    #[cfg(unix)]
    Socket(Socket),
}

#[derive(Debug)]
pub(crate) struct Endpoint {
    transport: EndpointTransport,
    read_timeout: IoDeadline,
    write_timeout: IoDeadline,
}

#[derive(Debug)]
struct IoDeadline {
    timeout: Option<Duration>,
    sleep: Option<Pin<Box<tokio::time::Sleep>>>,
    last_deadline: Option<Instant>,
}

impl IoDeadline {
    fn new(timeout: Option<Duration>) -> Self {
        Self {
            timeout,
            sleep: None,
            last_deadline: None,
        }
    }

    /// Start on the first poll and preserve one deadline across Pending polls.
    /// Call `complete` after any Ready result so the next socket operation has
    /// its own duration, matching Go's SetRead/WriteDeadline-before-I/O calls.
    fn expired(&mut self, cx: &mut Context<'_>) -> bool {
        let Some(timeout) = self.timeout else {
            return false;
        };
        let sleep = self.sleep.get_or_insert_with(|| {
            let deadline = Instant::now() + timeout;
            self.last_deadline = Some(deadline);
            Box::pin(tokio::time::sleep_until(deadline.into()))
        });
        sleep.as_mut().poll(cx).is_ready()
    }

    fn complete(&mut self) {
        self.sleep = None;
    }

    fn last_deadline(&self) -> Option<Instant> {
        self.last_deadline
    }
}

/// Carries the last raw-socket deadlines through the MySQL TLS upgrade.
/// Go's TLS handshake inherits the deadlines set by the preceding greeting
/// read and SSLRequest write; it does not refresh them at handshake start.
#[derive(Debug)]
pub(crate) struct TlsHandshakeStream<S> {
    stream: S,
    read_deadline: Option<Pin<Box<tokio::time::Sleep>>>,
    write_deadline: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl<S> TlsHandshakeStream<S> {
    pub(crate) fn new(
        stream: S,
        read_deadline: Option<Instant>,
        write_deadline: Option<Instant>,
    ) -> Self {
        Self {
            stream,
            read_deadline: read_deadline.map(|d| Box::pin(tokio::time::sleep_until(d.into()))),
            write_deadline: write_deadline.map(|d| Box::pin(tokio::time::sleep_until(d.into()))),
        }
    }

    pub(crate) fn disable_deadlines(&mut self) {
        self.read_deadline = None;
        self.write_deadline = None;
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for TlsHandshakeStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this
            .read_deadline
            .as_mut()
            .is_some_and(|d| d.as_mut().poll(cx).is_ready())
        {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "MySQL TLS handshake read timed out",
            )));
        }
        Pin::new(&mut this.stream).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for TlsHandshakeStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this
            .write_deadline
            .as_mut()
            .is_some_and(|d| d.as_mut().poll(cx).is_ready())
        {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "MySQL TLS handshake write timed out",
            )));
        }
        Pin::new(&mut this.stream).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this
            .write_deadline
            .as_mut()
            .is_some_and(|d| d.as_mut().poll(cx).is_ready())
        {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "MySQL TLS handshake write timed out",
            )));
        }
        Pin::new(&mut this.stream).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}

impl Endpoint {
    fn new<T: Into<EndpointTransport>>(
        transport: T,
        read_timeout: Option<Duration>,
        write_timeout: Option<Duration>,
    ) -> Self {
        Self {
            transport: transport.into(),
            read_timeout: IoDeadline::new(read_timeout),
            write_timeout: IoDeadline::new(write_timeout),
        }
    }
}

/// This future will check that TcpStream is live.
///
/// This check is similar to a one, implemented by GitHub team for the go-sql-driver/mysql.
#[derive(Debug)]
struct CheckTcpStream<'a>(&'a mut TcpStream);

impl Future for CheckTcpStream<'_> {
    type Output = io::Result<()>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.0.poll_read_ready(cx) {
            Poll::Ready(Ok(())) => {
                // stream is readable
                let mut buf = [0_u8; 1];
                match self.0.try_read(&mut buf) {
                    Ok(0) => Poll::Ready(Err(io::Error::new(BrokenPipe, "broken pipe"))),
                    Ok(_) => Poll::Ready(Err(io::Error::new(Other, "stream should be empty"))),
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => Poll::Ready(Ok(())),
                    Err(err) => Poll::Ready(Err(err)),
                }
            }
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Pending => Poll::Ready(Ok(())),
        }
    }
}

impl Endpoint {
    #[cfg(unix)]
    fn is_socket(&self) -> bool {
        matches!(&self.transport, EndpointTransport::Socket(_))
    }

    /// Checks, that connection is alive.
    async fn check(&mut self) -> std::result::Result<(), IoError> {
        //return Ok(());
        match &mut self.transport {
            EndpointTransport::Plain(Some(stream)) => {
                CheckTcpStream(stream).await?;
                Ok(())
            }
            #[cfg(feature = "native-tls-tls")]
            EndpointTransport::Secure(tls_stream) => {
                CheckTcpStream(&mut tls_stream.get_mut().get_mut().get_mut().stream).await?;
                Ok(())
            }
            #[cfg(feature = "rustls-tls")]
            EndpointTransport::Secure(tls_stream) => {
                let stream = &mut tls_stream.get_mut().0.stream;
                CheckTcpStream(stream).await?;
                Ok(())
            }
            #[cfg(unix)]
            EndpointTransport::Socket(socket) => {
                let _ = socket.write(&[]).await?;
                Ok(())
            }
            EndpointTransport::Plain(None) => unreachable!(),
        }
    }

    #[cfg(any(feature = "native-tls-tls", feature = "rustls-tls"))]
    pub fn is_secure(&self) -> bool {
        matches!(&self.transport, EndpointTransport::Secure(_))
    }

    pub fn set_tcp_nodelay(&self, val: bool) -> io::Result<()> {
        match &self.transport {
            EndpointTransport::Plain(Some(stream)) => stream.set_nodelay(val)?,
            EndpointTransport::Plain(None) => unreachable!(),
            #[cfg(feature = "native-tls-tls")]
            EndpointTransport::Secure(stream) => stream
                .get_ref()
                .get_ref()
                .get_ref()
                .stream
                .set_nodelay(val)?,
            #[cfg(feature = "rustls-tls")]
            EndpointTransport::Secure(stream) => {
                let stream = &stream.get_ref().0.stream;
                stream.set_nodelay(val)?;
            }
            #[cfg(unix)]
            EndpointTransport::Socket(_) => (/* inapplicable */),
        }
        Ok(())
    }
}

impl From<TcpStream> for EndpointTransport {
    fn from(stream: TcpStream) -> Self {
        EndpointTransport::Plain(Some(stream))
    }
}

#[cfg(unix)]
impl From<Socket> for EndpointTransport {
    fn from(socket: Socket) -> Self {
        EndpointTransport::Socket(socket)
    }
}

#[cfg(feature = "native-tls-tls")]
impl From<tokio_native_tls::TlsStream<TlsHandshakeStream<TcpStream>>> for EndpointTransport {
    fn from(stream: tokio_native_tls::TlsStream<TlsHandshakeStream<TcpStream>>) -> Self {
        EndpointTransport::Secure(stream)
    }
}

/* TODO
#[cfg(feature = "rustls-tls")]
*/

impl AsyncRead for Endpoint {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::result::Result<(), tokio::io::Error>> {
        let this = self.get_mut();
        if this.read_timeout.expired(cx) {
            this.read_timeout.complete();
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "MySQL read timed out",
            )));
        }
        let result = with_interrupted!(match &mut this.transport {
            EndpointTransport::Plain(stream) => {
                Pin::new(stream.as_mut().unwrap()).poll_read(cx, buf)
            }
            #[cfg(feature = "native-tls-tls")]
            EndpointTransport::Secure(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(feature = "rustls-tls")]
            EndpointTransport::Secure(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(unix)]
            EndpointTransport::Socket(stream) => Pin::new(stream).poll_read(cx, buf),
        });
        if result.is_ready() {
            this.read_timeout.complete();
        }
        result
    }
}

impl AsyncWrite for Endpoint {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, tokio::io::Error>> {
        let this = self.get_mut();
        if this.write_timeout.expired(cx) {
            this.write_timeout.complete();
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "MySQL write timed out",
            )));
        }
        let result = with_interrupted!(match &mut this.transport {
            EndpointTransport::Plain(stream) => {
                Pin::new(stream.as_mut().unwrap()).poll_write(cx, buf)
            }
            #[cfg(feature = "native-tls-tls")]
            EndpointTransport::Secure(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(feature = "rustls-tls")]
            EndpointTransport::Secure(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(unix)]
            EndpointTransport::Socket(stream) => Pin::new(stream).poll_write(cx, buf),
        });
        if result.is_ready() {
            this.write_timeout.complete();
        }
        result
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<std::result::Result<(), tokio::io::Error>> {
        let this = self.get_mut();
        if this.write_timeout.expired(cx) {
            this.write_timeout.complete();
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "MySQL write timed out while flushing",
            )));
        }
        let result = with_interrupted!(match &mut this.transport {
            EndpointTransport::Plain(stream) => {
                Pin::new(stream.as_mut().unwrap()).poll_flush(cx)
            }
            #[cfg(feature = "native-tls-tls")]
            EndpointTransport::Secure(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(feature = "rustls-tls")]
            EndpointTransport::Secure(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(unix)]
            EndpointTransport::Socket(stream) => Pin::new(stream).poll_flush(cx),
        });
        if result.is_ready() {
            this.write_timeout.complete();
        }
        result
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<std::result::Result<(), tokio::io::Error>> {
        let this = self.get_mut();
        with_interrupted!(match &mut this.transport {
            EndpointTransport::Plain(stream) => {
                Pin::new(stream.as_mut().unwrap()).poll_shutdown(cx)
            }
            #[cfg(feature = "native-tls-tls")]
            EndpointTransport::Secure(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(feature = "rustls-tls")]
            EndpointTransport::Secure(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(unix)]
            EndpointTransport::Socket(stream) => Pin::new(stream).poll_shutdown(cx),
        })
    }
}

/// A Stream, connected to MySql server.
pub struct Stream {
    closed: bool,
    pub(crate) codec: Option<Box<Framed<Endpoint, PacketCodec>>>,
}

impl fmt::Debug for Stream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Stream (endpoint={:?})",
            self.codec.as_ref().unwrap().get_ref()
        )
    }
}

impl Stream {
    #[cfg(unix)]
    fn new(endpoint: Endpoint) -> Self {
        Self {
            closed: false,
            codec: Box::new(Framed::new(endpoint, PacketCodec::default())).into(),
        }
    }

    pub(crate) async fn connect_tcp(
        addr: &HostPortOrUrl,
        keepalive: Option<Duration>,
        connect_timeout: Option<Duration>,
        read_timeout: Option<Duration>,
        write_timeout: Option<Duration>,
    ) -> io::Result<Stream> {
        let connect = async {
            match addr {
                HostPortOrUrl::HostPort {
                    host,
                    port,
                    resolved_ips,
                } => match resolved_ips {
                    Some(ips) => {
                        let addrs = ips
                            .iter()
                            .map(|ip| SocketAddr::new(*ip, *port))
                            .collect::<Vec<_>>();
                        TcpStream::connect(&*addrs).await
                    }
                    None => TcpStream::connect((host.as_str(), *port)).await,
                },
                HostPortOrUrl::Url(url) => {
                    let addrs = url.socket_addrs(|| Some(DEFAULT_PORT))?;
                    TcpStream::connect(&*addrs).await
                }
            }
        };
        let tcp_stream = match connect_timeout {
            Some(timeout) => tokio::time::timeout(timeout, connect).await.map_err(|_| {
                io::Error::new(io::ErrorKind::TimedOut, "MySQL connect timed out")
            })??,
            None => connect.await?,
        };

        #[cfg(any(unix, windows))]
        if let Some(duration) = keepalive {
            socket2::SockRef::from(&tcp_stream)
                .set_tcp_keepalive(&socket2::TcpKeepalive::new().with_time(duration))?;
        }

        Ok(Stream {
            closed: false,
            codec: Box::new(Framed::new(
                Endpoint::new(tcp_stream, read_timeout, write_timeout),
                PacketCodec::default(),
            ))
            .into(),
        })
    }

    #[cfg(unix)]
    pub(crate) async fn connect_socket<P: AsRef<Path>>(
        path: P,
        connect_timeout: Option<Duration>,
        read_timeout: Option<Duration>,
        write_timeout: Option<Duration>,
    ) -> io::Result<Stream> {
        let connect = Socket::new(path);
        let socket = match connect_timeout {
            Some(timeout) => tokio::time::timeout(timeout, connect).await.map_err(|_| {
                io::Error::new(io::ErrorKind::TimedOut, "MySQL connect timed out")
            })??,
            None => connect.await?,
        };
        Ok(Stream::new(Endpoint::new(
            socket,
            read_timeout,
            write_timeout,
        )))
    }

    pub(crate) fn set_tcp_nodelay(&self, val: bool) -> io::Result<()> {
        self.codec.as_ref().unwrap().get_ref().set_tcp_nodelay(val)
    }

    pub(crate) async fn make_secure(
        &mut self,
        domain: String,
        tls_connector: &TlsConnector,
    ) -> crate::error::Result<()> {
        let codec = self.codec.take().unwrap();
        let FramedParts { mut io, codec, .. } = codec.into_parts();
        io.make_secure(domain, tls_connector).await?;
        let codec = Framed::new(io, codec);
        self.codec = Some(Box::new(codec));
        Ok(())
    }

    #[cfg(any(feature = "native-tls-tls", feature = "rustls-tls"))]
    pub(crate) fn is_secure(&self) -> bool {
        self.codec.as_ref().unwrap().get_ref().is_secure()
    }

    #[cfg(unix)]
    pub(crate) fn is_socket(&self) -> bool {
        self.codec.as_ref().unwrap().get_ref().is_socket()
    }

    pub(crate) fn reset_seq_id(&mut self) {
        if let Some(codec) = self.codec.as_mut() {
            codec.codec_mut().reset_seq_id();
        }
    }

    pub(crate) fn sync_seq_id(&mut self) {
        if let Some(codec) = self.codec.as_mut() {
            codec.codec_mut().sync_seq_id();
        }
    }

    pub(crate) fn get_max_allowed_packet(&self) -> usize {
        if let Some(codec) = self.codec.as_ref() {
            codec.codec().max_allowed_packet
        } else {
            unreachable!()
        }
    }

    pub(crate) fn set_max_allowed_packet(&mut self, max_allowed_packet: usize) {
        if let Some(codec) = self.codec.as_mut() {
            codec.codec_mut().max_allowed_packet = max_allowed_packet;
        }
    }

    pub(crate) fn compress(&mut self, level: crate::Compression) {
        if let Some(codec) = self.codec.as_mut() {
            codec.codec_mut().compress(level);
        }
    }

    /// Checks, that connection is alive.
    pub(crate) async fn check(&mut self) -> std::result::Result<(), IoError> {
        if let Some(codec) = self.codec.as_mut() {
            codec.get_mut().check().await?;
        }
        Ok(())
    }

    pub(crate) async fn close(mut self) -> std::result::Result<(), IoError> {
        self.closed = true;
        if let Some(mut codec) = self.codec {
            use futures_sink::Sink;
            std::future::poll_fn(|cx| match Pin::new(&mut *codec).poll_close(cx) {
                Poll::Ready(Err(IoError::Io(err))) if err.kind() == NotConnected => {
                    Poll::Ready(Ok(()))
                }
                x => x,
            })
            .await?;
        }
        Ok(())
    }
}

impl stream::Stream for Stream {
    type Item = std::result::Result<PooledBuf, IoError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if !self.closed {
            let item = ready!(Pin::new(self.codec.as_mut().unwrap()).poll_next(cx)).transpose()?;
            Poll::Ready(Ok(item).transpose())
        } else {
            Poll::Ready(None)
        }
    }
}

#[cfg(test)]
mod test {
    use std::{io, time::Duration};

    #[cfg(unix)] // no sane way to retrieve current keepalive value on windows
    #[tokio::test]
    async fn should_connect_with_keepalive() {
        use crate::{test_misc::get_opts, Conn};

        let duration = Duration::from_millis(42_000);
        let opts = get_opts()
            .tcp_keepalive(Some(duration))
            .prefer_socket(false);
        let mut conn: Conn = Conn::new(opts).await.unwrap();
        let stream = conn.stream_mut().unwrap();
        let endpoint = stream.codec.as_mut().unwrap().get_ref();
        let stream = match &endpoint.transport {
            super::EndpointTransport::Plain(Some(stream)) => stream,
            #[cfg(feature = "rustls-tls")]
            super::EndpointTransport::Secure(tls_stream) => &tls_stream.get_ref().0.stream,
            #[cfg(feature = "native-tls-tls")]
            super::EndpointTransport::Secure(tls_stream) => {
                &tls_stream.get_ref().get_ref().get_ref().stream
            }
            _ => unreachable!(),
        };
        let sock = unsafe {
            use std::os::unix::prelude::*;
            let raw = stream.as_raw_fd();
            socket2::Socket::from_raw_fd(raw)
        };

        assert_eq!(sock.tcp_keepalive_time().unwrap(), duration);

        std::mem::forget(sock);

        conn.disconnect().await.unwrap();
    }

    #[tokio::test]
    async fn tls_upgrade_uses_inherited_absolute_read_deadline() {
        use tokio::io::AsyncReadExt;

        let (stream, _peer) = tokio::io::duplex(8);
        let deadline = super::Instant::now() + Duration::from_millis(20);
        let mut stream = super::TlsHandshakeStream::new(stream, Some(deadline), None);
        tokio::time::sleep(Duration::from_millis(30)).await;

        let mut buf = [0; 1];
        let error = stream.read(&mut buf).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    }

    #[tokio::test]
    async fn tls_upgrade_stalled_read_and_write_use_inherited_deadlines() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (read_stream, _read_peer) = tokio::io::duplex(8);
        let deadline = super::Instant::now() + Duration::from_millis(20);
        let mut read_stream = super::TlsHandshakeStream::new(read_stream, Some(deadline), None);
        let mut buf = [0; 1];
        let error = read_stream.read(&mut buf).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);

        let (mut write_stream, _write_peer) = tokio::io::duplex(1);
        write_stream.write_all(&[0]).await.unwrap();
        let deadline = super::Instant::now() + Duration::from_millis(20);
        let mut write_stream = super::TlsHandshakeStream::new(write_stream, None, Some(deadline));
        let error = write_stream.write_all(&[1]).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    }
}

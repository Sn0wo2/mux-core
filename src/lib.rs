use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::io;
use std::num::NonZeroUsize;
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Poll, Waker};

use futures_util::StreamExt;
use futures_util::future::{Either, select};
use futures_util::stream::FuturesUnordered;
use socksaddr::{ParseError, SocksAddr};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, split};
use tokio::sync::{Semaphore, mpsc};
use tokio_util::compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};

const MAX_H2_STREAMS: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(clippy::exhaustive_enums)]
pub enum Mode {
    SingMux,
    MuxCool,
}

#[derive(Debug, Clone, Copy)]
#[allow(clippy::exhaustive_structs)]
pub struct Config {
    pub max_streams: usize,
    pub max_buffered_per_stream: usize,
    pub max_queued_output: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_streams: 512,
            max_buffered_per_stream: 4 * 1024 * 1024,
            max_queued_output: 8 * 1024 * 1024,
        }
    }
}

type StreamTask = Pin<Box<dyn Future<Output = (u32, io::Result<()>)>>>;

enum SingProtocol {
    Smux,
    Yamux,
    H2mux,
}

enum SmuxEvent<T: AsyncRead + AsyncWrite + Unpin> {
    Worker(async_smux::error::MuxResult<()>),
    Accepted(Option<async_smux::MuxStream<T>>),
    StreamDone(u32, io::Result<()>),
}

enum YamuxEvent {
    Inbound(Option<yamux::Result<yamux::Stream>>),
    StreamDone(u32, io::Result<()>),
}

#[derive(Clone, Copy)]
enum StreamKind {
    Sing,
    H2,
}

pub async fn serve<I, C, F, S>(io: I, mode: Mode, config: Config, connector: C) -> io::Result<()>
where
    I: AsyncRead + AsyncWrite + Unpin + 'static,
    C: Fn(String, u16) -> F + Clone + 'static,
    F: Future<Output = io::Result<S>> + 'static,
    S: AsyncRead + AsyncWrite + Unpin + 'static,
{
    let config = Config {
        max_buffered_per_stream: config.max_buffered_per_stream.max(1),
        max_queued_output: config.max_queued_output.clamp(1, Semaphore::MAX_PERMITS),
        ..config
    };
    match mode {
        Mode::SingMux => {
            let mut io = io;
            match read_sing_protocol(&mut io).await? {
                SingProtocol::Smux => serve_smux(io, config, connector).await,
                SingProtocol::Yamux => serve_yamux(io, config, connector).await,
                SingProtocol::H2mux => serve_h2mux(io, config, connector).await,
            }
        }
        Mode::MuxCool => serve_mux_cool(io, config, connector).await,
    }
}

#[allow(clippy::single_call_fn)]
async fn read_sing_protocol<I>(io: &mut I) -> io::Result<SingProtocol>
where
    I: AsyncRead + AsyncWrite + Unpin,
{
    let mut request = [0_u8; 3];
    io.read_exact(&mut request[..2]).await?;
    let protocol = match request[0] {
        0 => request[1],
        1 => {
            io.read_exact(&mut request[2..]).await?;
            if request[2] != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "mux padding not supported, set padding: false",
                ));
            }
            request[1]
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported mux session version",
            ));
        }
    };
    match protocol {
        0 => Ok(SingProtocol::Smux),
        1 => Ok(SingProtocol::Yamux),
        2 => Ok(SingProtocol::H2mux),
        _ => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "mux protocol unsupported",
        )),
    }
}

#[allow(clippy::single_call_fn)]
async fn serve_smux<I, C, F, S>(io: I, config: Config, connector: C) -> io::Result<()>
where
    I: AsyncRead + AsyncWrite + Unpin + 'static,
    C: Fn(String, u16) -> F + Clone + 'static,
    F: Future<Output = io::Result<S>> + 'static,
    S: AsyncRead + AsyncWrite + Unpin + 'static,
{
    let mut builder = async_smux::MuxBuilder::server();
    builder
        .with_max_rx_queue(
            NonZeroUsize::new(config.max_streams.clamp(1, 64)).unwrap_or(NonZeroUsize::MIN),
        )
        .with_max_tx_queue(NonZeroUsize::new(1).unwrap_or(NonZeroUsize::MIN));
    let (_connector, mut acceptor, worker) = builder.with_connection(io).build();
    let mut worker = Box::pin(worker);
    let mut stream_tasks = FuturesUnordered::<StreamTask>::new();
    loop {
        let event = futures_util::future::poll_fn(|cx| {
            if let Poll::Ready(result) = worker.as_mut().poll(cx) {
                return Poll::Ready(SmuxEvent::Worker(result));
            }
            if !stream_tasks.is_empty()
                && let Poll::Ready(Some((sid, result))) = stream_tasks.poll_next_unpin(cx)
            {
                return Poll::Ready(SmuxEvent::StreamDone(sid, result));
            }
            if let Poll::Ready(stream) = acceptor.poll_next_unpin(cx) {
                return Poll::Ready(SmuxEvent::Accepted(stream));
            }
            Poll::Pending
        })
        .await;
        match event {
            SmuxEvent::Worker(result) => {
                if let Err(error) = result {
                    tracing::warn!("mux: smux session failed: {error}");
                }
                return Ok(());
            }
            SmuxEvent::Accepted(Some(stream)) => {
                let sid = stream.get_stream_id();
                if stream_tasks.len() >= config.max_streams {
                    tracing::warn!("mux: smux stream limit reached; refusing stream {sid}");
                    drop(stream);
                    continue;
                }
                let connector = connector.clone();
                stream_tasks.push(Box::pin(async move {
                    (
                        sid,
                        handle_proxy_stream(sid, stream, StreamKind::Sing, connector).await,
                    )
                }));
            }
            SmuxEvent::Accepted(None) => return Ok(()),
            SmuxEvent::StreamDone(sid, result) => {
                if let Err(error) = result {
                    tracing::warn!("mux: stream {sid} failed: {error:?}");
                }
            }
        }
    }
}

#[allow(clippy::single_call_fn)]
async fn serve_yamux<I, C, F, S>(io: I, config: Config, connector: C) -> io::Result<()>
where
    I: AsyncRead + AsyncWrite + Unpin,
    C: Fn(String, u16) -> F + Clone + 'static,
    F: Future<Output = io::Result<S>> + 'static,
    S: AsyncRead + AsyncWrite + Unpin + 'static,
{
    let mut yamux_config = yamux::Config::default();
    yamux_config.set_max_num_streams(config.max_streams);
    yamux_config
        .set_max_connection_receive_window(Some(config.max_streams.saturating_mul(256 * 1024)));
    let mut connection = yamux::Connection::new(io.compat(), yamux_config, yamux::Mode::Server);
    let mut stream_tasks = FuturesUnordered::<StreamTask>::new();
    loop {
        let event = futures_util::future::poll_fn(|cx| {
            if !stream_tasks.is_empty()
                && let Poll::Ready(Some((sid, result))) = stream_tasks.poll_next_unpin(cx)
            {
                return Poll::Ready(YamuxEvent::StreamDone(sid, result));
            }
            if let Poll::Ready(stream) = connection.poll_next_inbound(cx) {
                return Poll::Ready(YamuxEvent::Inbound(stream));
            }
            Poll::Pending
        })
        .await;
        match event {
            YamuxEvent::Inbound(Some(Ok(stream))) => {
                let sid = stream.id().val();
                if stream_tasks.len() >= config.max_streams {
                    tracing::warn!("mux: yamux stream limit reached; refusing stream {sid}");
                    drop(stream);
                    continue;
                }
                let stream = stream.compat();
                let connector = connector.clone();
                stream_tasks.push(Box::pin(async move {
                    (
                        sid,
                        handle_proxy_stream(sid, stream, StreamKind::Sing, connector).await,
                    )
                }));
            }
            YamuxEvent::Inbound(Some(Err(error))) => {
                tracing::warn!("mux: yamux session failed: {error}");
                return Ok(());
            }
            YamuxEvent::Inbound(None) => return Ok(()),
            YamuxEvent::StreamDone(sid, result) => {
                if let Err(error) = result {
                    tracing::warn!("mux: stream {sid} failed: {error:?}");
                }
            }
        }
    }
}

#[allow(clippy::single_call_fn)]
async fn serve_h2mux<I, C, F, S>(io: I, config: Config, connector: C) -> io::Result<()>
where
    I: AsyncRead + AsyncWrite + Unpin,
    C: Fn(String, u16) -> F + Clone + 'static,
    F: Future<Output = io::Result<S>> + 'static,
    S: AsyncRead + AsyncWrite + Unpin + 'static,
{
    h2mux_rs::serve(io, config.max_streams.min(MAX_H2_STREAMS), move |stream| {
        let connector = connector.clone();
        async move { handle_proxy_stream(0, stream, StreamKind::H2, connector).await }
    })
    .await
}

async fn handle_proxy_stream<I, C, F, S>(
    sid: u32,
    mut stream: I,
    kind: StreamKind,
    connector: C,
) -> io::Result<()>
where
    I: AsyncRead + AsyncWrite + Unpin,
    C: Fn(String, u16) -> F,
    F: Future<Output = io::Result<S>>,
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (flags, target, initial) = match kind {
        StreamKind::Sing => match read_sing_request(&mut stream).await {
            Ok(request) => request,
            Err(error) => {
                tracing::warn!("mux: stream {sid} request failed: {error:?}");
                write_stream_error(&mut stream, StreamKind::Sing, "invalid request").await?;
                return Ok(());
            }
        },
        StreamKind::H2 => match read_h2_request(&mut stream).await {
            Ok(request) => request,
            Err(error) => {
                tracing::warn!("h2mux: stream request failed: {error:?}");
                write_stream_error(&mut stream, StreamKind::H2, &error.to_string()).await?;
                return Ok(());
            }
        },
    };
    let is_udp = match kind {
        StreamKind::Sing => flags & 1 != 0,
        StreamKind::H2 => flags & 1 != 0,
    };
    if is_udp {
        let message = match kind {
            StreamKind::Sing => "udp is not supported",
            StreamKind::H2 => "UDP is unsupported",
        };
        tracing::warn!("mux: stream {sid} requests {message}");
        write_stream_error(&mut stream, kind, message).await?;
        return Ok(());
    }
    if matches!(kind, StreamKind::H2) && flags & !1 != 0 {
        write_stream_error(&mut stream, kind, "unsupported stream flags").await?;
        return Ok(());
    }
    if !target.is_routable() {
        let message = match kind {
            StreamKind::Sing => "invalid target",
            StreamKind::H2 => "invalid stream target",
        };
        tracing::warn!("mux: stream {sid} requests {message}");
        write_stream_error(&mut stream, kind, message).await?;
        return Ok(());
    }
    tracing::info!("mux: stream {sid} -> {target}");
    let host = target.host().into_owned();
    let port = target.port();
    let socket = match connector(host, port).await {
        Ok(socket) => socket,
        Err(error) => {
            tracing::warn!("mux: stream {sid} connect failed: {error:?}");
            let message = match kind {
                StreamKind::Sing => "connect failed".to_owned(),
                StreamKind::H2 => error.to_string(),
            };
            write_stream_error(&mut stream, kind, &message).await?;
            return Ok(());
        }
    };
    stream.write_all(&[0]).await?;
    match pump_stream(stream, socket, initial).await {
        Ok((up, down)) => tracing::info!("mux: stream {sid} finished (up={up}, down={down})"),
        Err(error) => tracing::warn!("mux: stream {sid} pump failed: {error}"),
    }
    Ok(())
}

#[allow(clippy::single_call_fn)]
async fn read_sing_request<S>(stream: &mut S) -> io::Result<(u16, SocksAddr, Vec<u8>)>
where
    S: AsyncRead + Unpin,
{
    let mut framed = Vec::new();
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        if let Some(first) = framed.first().copied()
            && let Some(payload) = framed.get(2..)
        {
            match SocksAddr::parse_socks(payload) {
                Ok((target, consumed)) => {
                    let remaining = framed.split_off(2 + consumed);
                    return Ok((u16::from(first), target, remaining));
                }
                Err(ParseError::Truncated) => {}
                Err(ParseError::Invalid(message)) => {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, message));
                }
            }
        }
        if framed.len() > 2 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "mux request too large",
            ));
        }
        let length = stream.read(&mut buffer).await?;
        if length == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated mux request",
            ));
        }
        let bytes = buffer.get(..length).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid mux request length")
        })?;
        framed.extend_from_slice(bytes);
    }
}

#[allow(clippy::single_call_fn)]
async fn read_h2_request<S>(stream: &mut S) -> io::Result<(u16, SocksAddr, Vec<u8>)>
where
    S: AsyncRead + Unpin,
{
    const MAX_HEADER_SIZE: usize = 2 + 259;
    let mut framed = Vec::new();
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        if framed.len() >= 2 {
            let flags = framed
                .get(..2)
                .and_then(|bytes| <[u8; 2]>::try_from(bytes).ok())
                .map(u16::from_be_bytes)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid h2mux flags"))?;
            let address = framed.get(2..).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid h2mux address")
            })?;
            match SocksAddr::parse_socks(address) {
                Ok((target, consumed)) => {
                    let remaining = framed.split_off(2 + consumed);
                    return Ok((flags, target, remaining));
                }
                Err(ParseError::Truncated) => {}
                Err(ParseError::Invalid(message)) => {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, message));
                }
            }
        }
        if framed.len() >= MAX_HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "h2mux stream request header is too large",
            ));
        }
        let length = stream.read(&mut buffer).await?;
        if length == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "h2mux stream ended before its request header",
            ));
        }
        let bytes = buffer.get(..length).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid h2mux request length")
        })?;
        framed.extend_from_slice(bytes);
    }
}

async fn write_stream_error<S>(stream: &mut S, kind: StreamKind, message: &str) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    let payload = match kind {
        StreamKind::Sing => {
            let mut payload = vec![0x01];
            let mut length = message.len() as u64;
            loop {
                let byte = (length & 0x7f) as u8;
                length >>= 7;
                payload.push(if length == 0 { byte } else { byte | 0x80 });
                if length == 0 {
                    break;
                }
            }
            payload.extend_from_slice(message.as_bytes());
            payload
        }
        StreamKind::H2 => h2mux_rs::error_response(message),
    };
    stream.write_all(&payload).await?;
    stream.shutdown().await
}

#[allow(clippy::single_call_fn)]
async fn pump_stream<I, S>(stream: I, socket: S, initial: Vec<u8>) -> io::Result<(u64, u64)>
where
    I: AsyncRead + AsyncWrite + Unpin,
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (mut stream_reader, mut stream_writer) = split(stream);
    let (mut socket_reader, mut socket_writer) = split(socket);
    let initial_up = initial.len() as u64;
    if !initial.is_empty() {
        socket_writer.write_all(&initial).await?;
    }
    let upload = async {
        let mut up = initial_up;
        let mut buffer = vec![0_u8; 64 * 1024];
        loop {
            let length = stream_reader.read(&mut buffer).await?;
            if length == 0 {
                socket_writer.shutdown().await?;
                return Ok::<u64, io::Error>(up);
            }
            let bytes = buffer.get(..length).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid mux stream read length")
            })?;
            socket_writer.write_all(bytes).await?;
            up += length as u64;
        }
    };
    let download = async {
        let mut down = 0_u64;
        let mut buffer = vec![0_u8; 64 * 1024];
        loop {
            let length = socket_reader.read(&mut buffer).await?;
            if length == 0 {
                stream_writer.shutdown().await?;
                return Ok::<u64, io::Error>(down);
            }
            let bytes = buffer.get(..length).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid socket read length")
            })?;
            stream_writer.write_all(bytes).await?;
            down += length as u64;
        }
    };
    match select(pin!(upload), pin!(download)).await {
        Either::Left((result, download)) => {
            let up = result?;
            let down = download.await?;
            Ok((up, down))
        }
        Either::Right((result, upload)) => {
            let down = result?;
            let up = upload.await?;
            Ok((up, down))
        }
    }
}

#[derive(Default)]
struct StreamShared {
    buffer: VecDeque<Vec<u8>>,
    buffered_len: usize,
    eof: bool,
    waker: Option<Waker>,
}

impl StreamShared {
    fn close_read(&mut self) {
        self.eof = true;
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}

type Shared = Rc<RefCell<StreamShared>>;
type CoolTask<S> = Rc<RefCell<Pin<Box<dyn Future<Output = CoolEvent<S>>>>>>;

struct CoolStream<S> {
    shared: Shared,
    fut: CoolTask<S>,
}

struct Job {
    bytes: Vec<u8>,
    _budget: Option<tokio::sync::OwnedSemaphorePermit>,
}

struct Outbox {
    tx: mpsc::Sender<Job>,
    budget: Arc<Semaphore>,
    capacity: usize,
}

impl Outbox {
    async fn send_framed(&self, bytes: Vec<u8>) {
        let budget = bytes.len().min(self.capacity);
        let permit = if budget > 0 {
            match Arc::clone(&self.budget)
                .acquire_many_owned(budget as u32)
                .await
            {
                Ok(permit) => Some(permit),
                Err(_) => return,
            }
        } else {
            None
        };
        drop(
            self.tx
                .send(Job {
                    bytes,
                    _budget: permit,
                })
                .await,
        );
    }

    async fn send_control(&self, bytes: Vec<u8>) {
        drop(
            self.tx
                .send(Job {
                    bytes,
                    _budget: None,
                })
                .await,
        );
    }
}

enum CoolEvent<S> {
    Raw(mux_cool_rs::Event),
    ReaderDone,
    WriterDone,
    Connected(u32, S),
    ConnectFailed(u32, io::Error),
    PumpDone(u32, io::Result<(u64, u64)>),
}

#[allow(clippy::single_call_fn)]
fn connect_future<C, F, S>(sid: u32, target: &SocksAddr, connector: C) -> CoolTask<S>
where
    C: Fn(String, u16) -> F + 'static,
    F: Future<Output = io::Result<S>> + 'static,
    S: 'static,
{
    let host = target.host().into_owned();
    let port = target.port();
    Rc::new(RefCell::new(Box::pin(async move {
        match connector(host, port).await {
            Ok(socket) => CoolEvent::Connected(sid, socket),
            Err(error) => CoolEvent::ConnectFailed(sid, error),
        }
    })))
}

#[allow(clippy::single_call_fn)]
async fn serve_mux_cool<I, C, F, S>(io: I, config: Config, connector: C) -> io::Result<()>
where
    I: AsyncRead + AsyncWrite + Unpin,
    C: Fn(String, u16) -> F + Clone + 'static,
    F: Future<Output = io::Result<S>> + 'static,
    S: AsyncRead + AsyncWrite + Unpin + 'static,
{
    let decoder = mux_cool_rs::Decoder;
    let (mut reader_io, mut writer_io) = split(io);
    let (event_tx, mut event_rx) = mpsc::channel(16);
    let (job_tx, mut job_rx) = mpsc::channel(16);
    let outbox = Rc::new(Outbox {
        tx: job_tx,
        budget: Arc::new(Semaphore::new(config.max_queued_output)),
        capacity: config.max_queued_output,
    });
    let reader_decoder = decoder;
    let mut reader = Box::pin(async move {
        let mut pending = Vec::new();
        let mut buffer = [0_u8; 16 * 1024];
        loop {
            let buffered = pending.len();
            match reader_decoder.decode(&mut pending) {
                Ok(events) if !events.is_empty() => {
                    for event in events {
                        if event_tx.send(CoolEvent::Raw(event)).await.is_err() {
                            return CoolEvent::ReaderDone;
                        }
                    }
                    continue;
                }
                Ok(_) if pending.len() < buffered => continue,
                Ok(_) => {}
                Err(error) => {
                    tracing::error!("mux: tunnel reader failed: {error:?}");
                    return CoolEvent::ReaderDone;
                }
            }
            match reader_io.read(&mut buffer).await {
                Ok(0) => {
                    tracing::info!("mux: tunnel ended");
                    return CoolEvent::ReaderDone;
                }
                Ok(length) => {
                    let Some(bytes) = buffer.get(..length) else {
                        tracing::error!("mux: invalid tunnel read length");
                        return CoolEvent::ReaderDone;
                    };
                    pending.extend_from_slice(bytes);
                }
                Err(error) => {
                    tracing::error!("mux: tunnel reader failed: {error:?}");
                    return CoolEvent::ReaderDone;
                }
            }
        }
    });
    let mut writer = Box::pin(async move {
        while let Some(job) = job_rx.recv().await {
            if let Err(error) = writer_io.write_all(&job.bytes).await {
                tracing::error!("mux: tunnel writer failed: {error:?}");
                return CoolEvent::WriterDone;
            }
            drop(job);
        }
        CoolEvent::WriterDone
    });
    let mut reader_done = false;
    let mut writer_done = false;
    let mut streams: HashMap<u32, CoolStream<S>> = HashMap::new();
    let mut tunnel_done = false;
    loop {
        if writer_done {
            break;
        }
        let (event, reader_finished, writer_finished) = futures_util::future::poll_fn(|cx| {
            if !reader_done && let Poll::Ready(event) = reader.as_mut().poll(cx) {
                return Poll::Ready((event, true, false));
            }
            if !writer_done && let Poll::Ready(event) = writer.as_mut().poll(cx) {
                return Poll::Ready((event, false, true));
            }
            if let Poll::Ready(event) = event_rx.poll_recv(cx) {
                return Poll::Ready((event.unwrap_or(CoolEvent::ReaderDone), false, false));
            }
            for stream in streams.values() {
                if let Poll::Ready(event) = stream.fut.borrow_mut().as_mut().poll(cx) {
                    return Poll::Ready((event, false, false));
                }
            }
            Poll::Pending
        })
        .await;
        if reader_finished {
            reader_done = true;
        }
        if writer_finished {
            writer_done = true;
        }
        match event {
            CoolEvent::Raw(mux_cool_rs::Event::Open { sid, target }) => {
                if streams.contains_key(&sid) {
                    continue;
                }
                if streams.len() >= config.max_streams {
                    outbox.send_control(decoder.reset_frame(sid)).await;
                    continue;
                }
                let shared: Shared = Rc::default();
                if !target.is_routable() {
                    outbox.send_control(decoder.reset_frame(sid)).await;
                    continue;
                }
                streams.insert(
                    sid,
                    CoolStream {
                        shared,
                        fut: connect_future(sid, &target, connector.clone()),
                    },
                );
            }
            CoolEvent::Raw(mux_cool_rs::Event::Data { sid, data }) => {
                let data_len = data.len();
                let mut overflow = false;
                if let Some(stream) = streams.get(&sid) {
                    let mut shared = stream.shared.borrow_mut();
                    if shared.eof
                        || shared.buffered_len.saturating_add(data_len)
                            > config.max_buffered_per_stream
                    {
                        overflow = true;
                    } else {
                        shared.buffered_len += data_len;
                        shared.buffer.push_back(data);
                        if let Some(waker) = shared.waker.take() {
                            waker.wake();
                        }
                    }
                }
                if overflow {
                    tracing::warn!("mux: stream {sid} exceeded its buffer, resetting");
                    outbox.send_control(decoder.reset_frame(sid)).await;
                    remove_cool_stream(&mut streams, sid);
                }
            }
            CoolEvent::Raw(mux_cool_rs::Event::Eof { sid }) => {
                if let Some(stream) = streams.get(&sid) {
                    stream.shared.borrow_mut().close_read();
                }
            }
            CoolEvent::Raw(mux_cool_rs::Event::Reset { sid }) => {
                remove_cool_stream(&mut streams, sid);
            }
            CoolEvent::Raw(mux_cool_rs::Event::Reject { sid }) => {
                tracing::info!("mux: stream {sid} requests udp, rejecting");
                outbox.send_control(decoder.reset_frame(sid)).await;
                remove_cool_stream(&mut streams, sid);
            }
            CoolEvent::ReaderDone => {
                tunnel_done = true;
                streams.clear();
            }
            CoolEvent::WriterDone => {}
            CoolEvent::Connected(sid, socket) => {
                let Some(stream) = streams.get_mut(&sid) else {
                    continue;
                };
                let shared = Rc::clone(&stream.shared);
                let outbox = Rc::clone(&outbox);
                stream.fut = Rc::new(RefCell::new(Box::pin(async move {
                    CoolEvent::PumpDone(
                        sid,
                        pump_cool_stream(sid, socket, shared, outbox, decoder).await,
                    )
                })));
            }
            CoolEvent::ConnectFailed(sid, error) => {
                tracing::warn!("mux: stream {sid} connect failed: {error:?}");
                outbox.send_control(decoder.reset_frame(sid)).await;
                remove_cool_stream(&mut streams, sid);
            }
            CoolEvent::PumpDone(sid, result) => match result {
                Ok((up, down)) => {
                    tracing::info!("mux: stream {sid} finished (up={up}, down={down})");
                    remove_cool_stream(&mut streams, sid);
                }
                Err(error) => {
                    tracing::warn!("mux: stream {sid} pump failed: {error}");
                    outbox.send_control(decoder.reset_frame(sid)).await;
                    remove_cool_stream(&mut streams, sid);
                }
            },
        }
        if tunnel_done && streams.is_empty() {
            break;
        }
    }
    drop(outbox);
    Ok(())
}

fn remove_cool_stream<S>(streams: &mut HashMap<u32, CoolStream<S>>, sid: u32) {
    if let Some(stream) = streams.remove(&sid) {
        stream.shared.borrow_mut().close_read();
    }
}

#[allow(clippy::single_call_fn)]
async fn pump_cool_stream<S>(
    sid: u32,
    socket: S,
    shared: Shared,
    outbox: Rc<Outbox>,
    decoder: mux_cool_rs::Decoder,
) -> io::Result<(u64, u64)>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (mut reader, mut writer) = split(socket);
    let read_half = async {
        let mut down = 0_u64;
        let mut buffer = vec![0_u8; 64 * 1024];
        loop {
            let length = reader.read(&mut buffer).await?;
            if length == 0 {
                outbox.send_control(decoder.eof_frame(sid)).await;
                return Ok::<u64, io::Error>(down);
            }
            let bytes = buffer.get(..length).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid socket read length")
            })?;
            let mut data = bytes;
            while !data.is_empty() {
                let chunk = data.len().min(decoder.chunk_limit());
                let (head, tail) = data.split_at(chunk);
                outbox.send_framed(decoder.data_frame(sid, head)).await;
                data = tail;
            }
            down += length as u64;
        }
    };
    let write_half = async {
        let mut up = 0_u64;
        loop {
            let has_data = futures_util::future::poll_fn(|cx| {
                let mut shared = shared.borrow_mut();
                if shared.buffered_len > 0 {
                    return Poll::Ready(true);
                }
                if shared.eof {
                    return Poll::Ready(false);
                }
                shared.waker = Some(cx.waker().clone());
                Poll::Pending
            })
            .await;
            if !has_data {
                writer.shutdown().await?;
                return Ok::<u64, io::Error>(up);
            }
            let chunk = {
                let mut shared = shared.borrow_mut();
                let data = shared.buffer.pop_front().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "buffer length does not match stream data",
                    )
                })?;
                shared.buffered_len -= data.len();
                data
            };
            writer.write_all(&chunk).await?;
            up += chunk.len() as u64;
        }
    };
    match select(pin!(read_half), pin!(write_half)).await {
        Either::Left((result, write_half)) => {
            let down = result?;
            let up = write_half.await?;
            Ok((up, down))
        }
        Either::Right((result, read_half)) => {
            let up = result?;
            let down = read_half.await?;
            Ok((up, down))
        }
    }
}

use std::future::Future;
use std::io;
use std::num::NonZeroUsize;
use std::pin::pin;

use futures_util::future::poll_fn;
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, StreamExt};
use socksaddr::{ParseError, SocksAddr};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_util::compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};

use crate::{Config, utils::write_error};

#[derive(Clone, Copy)]
enum StreamKind {
    Sing,
    H2,
}

#[allow(clippy::single_call_fn)]
pub(super) async fn serve<I, C, F, S>(mut io: I, config: Config, connector: C) -> io::Result<()>
where
    I: AsyncRead + AsyncWrite + Unpin + 'static,
    C: Fn(String, u16) -> F + Clone + 'static,
    F: Future<Output = io::Result<S>> + 'static,
    S: AsyncRead + AsyncWrite + Unpin + 'static,
{
    let mut header = [0; 2];
    io.read_exact(&mut header).await?;
    let [version, protocol] = header;
    match version {
        0 => {}
        1 => {
            if io.read_u8().await? != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "mux padding not supported, set padding: false",
                ));
            }
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported mux session version",
            ));
        }
    }
    match protocol {
        0 => {
            let mut builder = async_smux::MuxBuilder::server();
            builder
                .with_max_rx_queue(
                    NonZeroUsize::new(config.max_streams.clamp(1, 64)).unwrap_or(NonZeroUsize::MIN),
                )
                .with_max_tx_queue(NonZeroUsize::MIN);
            let (_connector, mut acceptor, worker) = builder.with_connection(io).build();
            let mut worker = pin!(worker);
            let mut stream_tasks = FuturesUnordered::new();
            loop {
                tokio::select! {
                    biased;
                    result = &mut worker => {
                        if let Err(error) = result {
                            tracing::warn!("mux: smux session failed: {error}");
                        }
                        return Ok(());
                    }
                    Some((sid, result)) = stream_tasks.next(), if !stream_tasks.is_empty() => {
                        if let Err(error) = result {
                            tracing::warn!("mux: stream {sid} failed: {error:?}");
                        }
                    }
                    stream = acceptor.next() => {
                        let Some(stream) = stream else {
                            return Ok(());
                        };
                        let sid = stream.get_stream_id();
                        if stream_tasks.len() >= config.max_streams {
                            tracing::warn!("mux: smux stream limit reached; refusing stream {sid}");
                            continue;
                        }
                        let connector = connector.clone();
                        stream_tasks.push(async move {
                            (sid, handle_proxy_stream(sid, stream, StreamKind::Sing, connector).await)
                        }.boxed_local());
                    }
                }
            }
        }
        1 => {
            let mut yamux_config = yamux::Config::default();
            yamux_config.set_max_num_streams(config.max_streams);
            yamux_config.set_max_connection_receive_window(Some(
                config.max_streams.saturating_mul(256 * 1024),
            ));
            let mut connection =
                yamux::Connection::new(io.compat(), yamux_config, yamux::Mode::Server);
            let mut stream_tasks = FuturesUnordered::new();
            loop {
                tokio::select! {
                    biased;
                    Some((sid, result)) = stream_tasks.next(), if !stream_tasks.is_empty() => {
                        if let Err(error) = result {
                            tracing::warn!("mux: stream {sid} failed: {error:?}");
                        }
                    }
                    stream = poll_fn(|cx| connection.poll_next_inbound(cx)) => {
                        match stream {
                            Some(Ok(stream)) => {
                                let sid = stream.id().val();
                                if stream_tasks.len() >= config.max_streams {
                                    tracing::warn!("mux: yamux stream limit reached; refusing stream {sid}");
                                    continue;
                                }
                                let connector = connector.clone();
                                stream_tasks.push(async move {
                                    (sid, handle_proxy_stream(sid, stream.compat(), StreamKind::Sing, connector).await)
                                }.boxed_local());
                            }
                            Some(Err(error)) => {
                                tracing::warn!("mux: yamux session failed: {error}");
                                return Ok(());
                            }
                            None => return Ok(()),
                        }
                    }
                }
            }
        }
        2 => {
            h2mux_rs::serve(io, config.max_streams.min(64), move |stream| {
                let connector = connector.clone();
                async move { handle_proxy_stream(0, stream, StreamKind::H2, connector).await }
            })
            .await
        }
        _ => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "mux protocol unsupported",
        )),
    }
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
    let (flags, target, initial) = match async {
        let mut framed = Vec::with_capacity(16 * 1024);
        loop {
            if let &[first, second, ref address @ ..] = framed.as_slice() {
                match SocksAddr::parse_socks(address) {
                    Ok((target, consumed)) => {
                        return Ok((
                            match kind {
                                StreamKind::Sing => u16::from(first),
                                StreamKind::H2 => u16::from_be_bytes([first, second]),
                            },
                            target,
                            framed.split_off(2 + consumed),
                        ));
                    }
                    Err(ParseError::Truncated) => {}
                    Err(ParseError::Invalid(message)) => {
                        return Err(io::Error::new(io::ErrorKind::InvalidData, message));
                    }
                }
            }
            if match kind {
                StreamKind::Sing => framed.len() > 2 * 1024,
                StreamKind::H2 => framed.len() >= 2 + 259,
            } {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    match kind {
                        StreamKind::Sing => "mux request too large",
                        StreamKind::H2 => "h2mux stream request header is too large",
                    },
                ));
            }
            if stream.read_buf(&mut framed).await? == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    match kind {
                        StreamKind::Sing => "truncated mux request",
                        StreamKind::H2 => "h2mux stream ended before its request header",
                    },
                ));
            }
        }
    }
    .await
    {
        Ok(request) => request,
        Err(error) => {
            match kind {
                StreamKind::Sing => {
                    tracing::warn!("mux: stream {sid} request failed: {error:?}");
                    write_error(&mut stream, "invalid request").await?;
                }
                StreamKind::H2 => {
                    tracing::warn!("h2mux: stream request failed: {error:?}");
                    write_error(&mut stream, &error.to_string()).await?;
                }
            }
            return Ok(());
        }
    };
    if flags & 1 != 0 {
        let message = match kind {
            StreamKind::Sing => "udp is not supported",
            StreamKind::H2 => "UDP is unsupported",
        };
        tracing::warn!("mux: stream {sid} requests {message}");
        write_error(&mut stream, message).await?;
        return Ok(());
    }
    if matches!(kind, StreamKind::H2) && flags & !1 != 0 {
        write_error(&mut stream, "unsupported stream flags").await?;
        return Ok(());
    }
    if !target.is_routable() {
        let message = match kind {
            StreamKind::Sing => "invalid target",
            StreamKind::H2 => "invalid stream target",
        };
        tracing::warn!("mux: stream {sid} requests {message}");
        write_error(&mut stream, message).await?;
        return Ok(());
    }
    tracing::info!("mux: stream {sid} -> {target}");
    let mut socket = match connector(target.host().into_owned(), target.port()).await {
        Ok(socket) => socket,
        Err(error) => {
            tracing::warn!("mux: stream {sid} connect failed: {error:?}");
            let message = match kind {
                StreamKind::Sing => "connect failed".to_owned(),
                StreamKind::H2 => error.to_string(),
            };
            write_error(&mut stream, &message).await?;
            return Ok(());
        }
    };
    stream.write_all(&[0]).await?;
    let initial_up = initial.len() as u64;
    match async {
        socket.write_all(&initial).await?;
        drop(initial);
        tokio::io::copy_bidirectional_with_sizes(&mut stream, &mut socket, 64 * 1024, 64 * 1024)
            .await
    }
    .await
    {
        Ok((up, down)) => tracing::info!(
            "mux: stream {sid} finished (up={}, down={down})",
            up + initial_up
        ),
        Err(error) => tracing::warn!("mux: stream {sid} pump failed: {error}"),
    }
    Ok(())
}

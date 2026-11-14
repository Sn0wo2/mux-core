use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::future::{Future, poll_fn};
use std::io;
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Poll, Waker};

use futures_util::Stream;
use futures_util::future::{select, try_join};
use futures_util::stream::unfold;
use mux_cool_rs::{Decoder, Event};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, split};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

use crate::Config;

#[derive(Default)]
struct StreamShared {
    buffer: VecDeque<Vec<u8>>,
    buffered_len: usize,
    eof: bool,
    waker: Option<Waker>,
}

struct CoolStream<S> {
    shared: Rc<RefCell<StreamShared>>,
    fut: Pin<Box<dyn Future<Output = CoolEvent<S>>>>,
}

enum CoolEvent<S> {
    Raw(Vec<Event>),
    Connected(u32, io::Result<S>),
    PumpDone(u32, io::Result<(u64, u64)>),
}

#[allow(clippy::single_call_fn)]
pub(super) async fn serve<I, C, F, S>(io: I, config: Config, connector: C) -> io::Result<()>
where
    I: AsyncRead + AsyncWrite + Unpin,
    C: Fn(String, u16) -> F + Clone + 'static,
    F: Future<Output = io::Result<S>> + 'static,
    S: AsyncRead + AsyncWrite + Unpin + 'static,
{
    let (reader_io, mut writer_io) = split(io);
    let (frame_tx, mut frame_rx) = mpsc::channel::<(Vec<u8>, Option<OwnedSemaphorePermit>)>(16);
    let budget = Arc::new(Semaphore::new(config.max_queued_output));
    let mut reader = pin!(unfold(
        (reader_io, Vec::with_capacity(16 * 1024)),
        |(mut reader_io, mut pending)| async move {
            loop {
                let buffered = pending.len();
                match Decoder.decode(&mut pending) {
                    Ok(events) if !events.is_empty() => {
                        return Some((events, (reader_io, pending)));
                    }
                    Ok(_) if pending.len() < buffered => continue,
                    Ok(_) => {}
                    Err(error) => {
                        tracing::error!("mux: tunnel reader failed: {error:?}");
                        return None;
                    }
                }
                match reader_io.read_buf(&mut pending).await {
                    Ok(0) => {
                        tracing::info!("mux: tunnel ended");
                        return None;
                    }
                    Ok(_) => {}
                    Err(error) => {
                        tracing::error!("mux: tunnel reader failed: {error:?}");
                        return None;
                    }
                }
            }
        },
    ));
    let writer = pin!(async {
        while let Some((bytes, permit)) = frame_rx.recv().await {
            if let Err(error) = writer_io.write_all(&bytes).await {
                tracing::error!("mux: tunnel writer failed: {error:?}");
                return;
            }
            drop(permit);
        }
    });
    let session = async {
        let mut streams: HashMap<u32, CoolStream<S>> = HashMap::new();
        loop {
            let Some(event) = poll_fn(|cx| {
                if let Poll::Ready(events) = reader.as_mut().poll_next(cx) {
                    return Poll::Ready(events.map(CoolEvent::Raw));
                }
                for stream in streams.values_mut() {
                    if let Poll::Ready(event) = stream.fut.as_mut().poll(cx) {
                        return Poll::Ready(Some(event));
                    }
                }
                Poll::Pending
            })
            .await
            else {
                break;
            };
            match event {
                CoolEvent::Raw(events) => {
                    for event in events {
                        match event {
                            Event::Open { sid, target } => {
                                if streams.contains_key(&sid) {
                                    continue;
                                }
                                if streams.len() >= config.max_streams || !target.is_routable() {
                                    drop(frame_tx.send((Decoder.reset_frame(sid), None)).await);
                                    continue;
                                }
                                let connector = connector.clone();
                                streams.insert(
                                    sid,
                                    CoolStream {
                                        shared: Rc::new(RefCell::new(StreamShared::default())),
                                        fut: Box::pin(async move {
                                            CoolEvent::Connected(
                                                sid,
                                                connector(
                                                    target.host().into_owned(),
                                                    target.port(),
                                                )
                                                .await,
                                            )
                                        }),
                                    },
                                );
                            }
                            Event::Data { sid, data } => {
                                let mut overflow = false;
                                if let Some(stream) = streams.get(&sid) {
                                    let mut shared = stream.shared.borrow_mut();
                                    if shared.eof
                                        || shared.buffered_len.saturating_add(data.len())
                                            > config.max_buffered_per_stream
                                    {
                                        overflow = true;
                                    } else {
                                        shared.buffered_len += data.len();
                                        shared.buffer.push_back(data);
                                        if let Some(waker) = shared.waker.take() {
                                            waker.wake();
                                        }
                                    }
                                }
                                if overflow {
                                    tracing::warn!(
                                        "mux: stream {sid} exceeded its buffer, resetting"
                                    );
                                    drop(frame_tx.send((Decoder.reset_frame(sid), None)).await);
                                    streams.remove(&sid);
                                }
                            }
                            Event::Eof { sid } => {
                                if let Some(stream) = streams.get(&sid) {
                                    let mut shared = stream.shared.borrow_mut();
                                    shared.eof = true;
                                    if let Some(waker) = shared.waker.take() {
                                        waker.wake();
                                    }
                                }
                            }
                            Event::Reset { sid } => {
                                streams.remove(&sid);
                            }
                            Event::Reject { sid } => {
                                tracing::info!("mux: stream {sid} requests udp, rejecting");
                                drop(frame_tx.send((Decoder.reset_frame(sid), None)).await);
                                streams.remove(&sid);
                            }
                        }
                    }
                }
                CoolEvent::Connected(sid, result) => match result {
                    Ok(socket) => {
                        let Some(stream) = streams.get_mut(&sid) else {
                            continue;
                        };
                        let shared = Rc::clone(&stream.shared);
                        let frame_tx = frame_tx.clone();
                        let budget = Arc::clone(&budget);
                        stream.fut = Box::pin(async move {
                            let (mut socket_reader, mut socket_writer) = split(socket);
                            CoolEvent::PumpDone(
                                sid,
                                try_join(
                                    async {
                                        let mut down = 0_u64;
                                        let mut buffer = Vec::with_capacity(64 * 1024);
                                        loop {
                                            buffer.clear();
                                            if socket_reader.read_buf(&mut buffer).await? == 0 {
                                                drop(
                                                    frame_tx
                                                        .send((Decoder.eof_frame(sid), None))
                                                        .await,
                                                );
                                                return Ok(down);
                                            }
                                            for chunk in buffer.chunks(Decoder.chunk_limit()) {
                                                let frame = Decoder.data_frame(sid, chunk);
                                                let Ok(permit) = Arc::clone(&budget)
                                                    .acquire_many_owned(u32::try_from(
                                                        frame.len().min(config.max_queued_output),
                                                    )
                                                    .unwrap_or(u32::MAX))
                                                    .await
                                                else {
                                                    continue;
                                                };
                                                drop(frame_tx.send((frame, Some(permit))).await);
                                            }
                                            down += buffer.len() as u64;
                                        }
                                    },
                                    async {
                                        let mut up = 0_u64;
                                        loop {
                                            if !poll_fn(|cx| {
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
                                            .await
                                            {
                                                socket_writer.shutdown().await?;
                                                return Ok(up);
                                            }
                                            let chunk = {
                                                let mut shared = shared.borrow_mut();
                                                let data = shared
                                                    .buffer
                                                    .pop_front()
                                                    .ok_or_else(|| {
                                                        io::Error::new(
                                                            io::ErrorKind::InvalidData,
                                                            "buffer length does not match stream data",
                                                        )
                                                    })?;
                                                shared.buffered_len -= data.len();
                                                data
                                            };
                                            socket_writer.write_all(&chunk).await?;
                                            up += chunk.len() as u64;
                                        }
                                    },
                                )
                                .await,
                            )
                        });
                    }
                    Err(error) => {
                        tracing::warn!("mux: stream {sid} connect failed: {error:?}");
                        drop(frame_tx.send((Decoder.reset_frame(sid), None)).await);
                        streams.remove(&sid);
                    }
                },
                CoolEvent::PumpDone(sid, result) => match result {
                    Ok((up, down)) => {
                        tracing::info!("mux: stream {sid} finished (up={up}, down={down})");
                        streams.remove(&sid);
                    }
                    Err(error) => {
                        tracing::warn!("mux: stream {sid} pump failed: {error}");
                        drop(frame_tx.send((Decoder.reset_frame(sid), None)).await);
                        streams.remove(&sid);
                    }
                },
            }
        }
    };
    select(pin!(session), writer).await;
    Ok(())
}

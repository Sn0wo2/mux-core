mod cool;
mod sing;
mod utils;

use std::future::Future;
use std::io;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::Semaphore;

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
        Mode::SingMux => sing::serve(io, config, connector).await,
        Mode::MuxCool => cool::serve(io, config, connector).await,
    }
}

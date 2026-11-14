use std::io;

use tokio::io::{AsyncWrite, AsyncWriteExt};

pub(super) async fn write_error<S>(stream: &mut S, message: &str) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    stream.write_all(&h2mux_rs::error_response(message)).await?;
    stream.shutdown().await
}

use std::io::Error as IoError;

use async_net::unix::UnixListener;
use futures_util::{StreamExt, AsyncWriteExt};

/// initialize if monitoring flag is set
pub(crate) async fn init_monitoring() -> Result<(), IoError> {
    let listener = UnixListener::bind("/tmp/socket")?;
    let mut incoming = listener.incoming();
    while let Some(stream) = incoming.next().await {
        let mut stream = stream?;
        stream.write_all(b"hello").await?;
    }

    Ok(())
}

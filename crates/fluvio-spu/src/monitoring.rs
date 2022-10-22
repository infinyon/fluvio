use std::io::Error as IoError;

use async_net::unix::UnixListener;
use futures_util::{StreamExt};

use crate::core::metrics::{SpuMetrics};

/// initialize if monitoring flag is set
pub(crate) async fn init_monitoring(metrics: &SpuMetrics) -> Result<(), IoError> {
    let listener = UnixListener::bind("/tmp/socket")?;
    let mut incoming = listener.incoming();
    while let Some(stream) = incoming.next().await {
        let mut stream = stream?;

        export::metrics(&mut stream, &metrics).await?;
    }

    Ok(())
}

mod export {

    use std::io::Error as IoError;

    use async_net::unix::UnixStream;
    use futures_util::AsyncWriteExt;
    use serde::Serialize;

    use crate::core::metrics::SpuMetrics;

    #[derive(Serialize, Default)]
    struct Metrics {
        records_read: u64,
        records_write: u64,
        bytes_read: u64,
        bytes_written: u64,
    }

    pub(crate) async fn metrics(
        stream: &mut UnixStream,
        metrics: &SpuMetrics,
    ) -> Result<(), IoError> {
        let out = Metrics {
            records_read: metrics.records_read(),
            records_write: metrics.records_write(),
            bytes_read: metrics.bytes_read(),
            bytes_written: metrics.bytes_written(),
        };

        let bytes = serde_json::to_vec_pretty(&out)?;
        stream.write_all(&bytes).await?;

        Ok(())
    }
}

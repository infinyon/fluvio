use std::io::Error as IoError;

use async_net::unix::UnixListener;
use futures_util::{StreamExt, AsyncWriteExt};

use crate::core::metrics;

/// initialize if monitoring flag is set
pub(crate) async fn init_monitoring() -> Result<(), IoError> {
    use export::Metrics;

    let mut metrics = Metrics::default();
    let listener = UnixListener::bind("/tmp/socket")?;
    let mut incoming = listener.incoming();
    while let Some(stream) = incoming.next().await {
        let mut stream = stream?;

        let bytes = serde_json::to_vec_pretty(&metrics)?;
        stream.write_all(&bytes).await?;
        metrics.total_counter += 1;
    }

    Ok(())
}

mod export {

    use serde::Serialize;

    #[derive(Serialize, Default)]
    pub(crate) struct Metrics {
        pub total_counter: u64,
    }
}

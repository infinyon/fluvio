use std::io::Error as IoError;

use async_net::unix::UnixListener;
use fluvio_future::task::spawn;
use futures_util::{StreamExt};

use fluvio_types::defaults::SPU_MONITORING_UNIX_SOCKET;

use crate::core::DefaultSharedGlobalContext;

pub(crate) async fn init_monitoring(ctx: DefaultSharedGlobalContext) {
    spawn(async move {
        if let Err(err) = start_monitoring(ctx).await {
            println!("error running monitoring: {}", err);
        }
    });
}

/// initialize if monitoring flag is set
async fn start_monitoring(ctx: DefaultSharedGlobalContext) -> Result<(), IoError> {
    /*
    if std::env::var("FLUVIO_METRIC").is_err() {
        println!("fluvio metric is not set");
        return Ok(());
    }
    */

    println!(
        "fluvio metric is set, using: {}",
        SPU_MONITORING_UNIX_SOCKET
    );

    let listener = UnixListener::bind(SPU_MONITORING_UNIX_SOCKET)?;
    let mut incoming = listener.incoming();

    let metrics = ctx.metrics();
    while let Some(stream) = incoming.next().await {
        let mut stream = stream?;

        export::metrics(&mut stream, metrics).await?;
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
        records_written: u64,
        bytes_read: u64,
        bytes_written: u64,
    }

    pub(crate) async fn metrics(
        stream: &mut UnixStream,
        metrics: &SpuMetrics,
    ) -> Result<(), IoError> {
        let out = Metrics {
            records_read: metrics.records_read(),
            records_written: metrics.records_write(),
            bytes_read: metrics.bytes_read(),
            bytes_written: metrics.bytes_written(),
        };

        let bytes = serde_json::to_vec_pretty(&out)?;
        stream.write_all(&bytes).await?;

        Ok(())
    }
}

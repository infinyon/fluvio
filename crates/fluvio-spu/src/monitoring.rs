use std::io::Error as IoError;

use async_net::unix::UnixListener;
use fluvio_future::task::spawn;
use futures_util::{StreamExt, AsyncWriteExt};

use fluvio_types::defaults::SPU_MONITORING_UNIX_SOCKET;

use crate::core::{DefaultSharedGlobalContext, metrics::SpuMetrics};

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

    let metrics: &SpuMetrics = &ctx.metrics();
    while let Some(stream) = incoming.next().await {
        let mut stream = stream?;

        println!("metrics: {:?}", metrics);
        let bytes = serde_json::to_vec_pretty(metrics)?;
        stream.write_all(&bytes).await?;
    }

    Ok(())
}

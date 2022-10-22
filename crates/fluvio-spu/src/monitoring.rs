use std::io::Error as IoError;

use async_net::unix::UnixListener;

use futures_util::{StreamExt, AsyncWriteExt};
use fluvio_types::defaults::SPU_MONITORING_UNIX_SOCKET;
use fluvio_future::task::spawn;

use crate::core::{DefaultSharedGlobalContext, metrics::SpuMetrics};

pub(crate) fn init_monitoring(ctx: DefaultSharedGlobalContext) {
    spawn(async move {
        if let Err(err) = start_monitoring(ctx).await {
            println!("error running monitoring: {}", err);
        }
    });
}

/// initialize if monitoring flag is set
async fn start_monitoring(ctx: DefaultSharedGlobalContext) -> Result<(), IoError> {
    let metric_out_path = match std::env::var("FLUVIO_METRIC_SPU") {
        Ok(path) => {
            println!("using metric path: {}", path);
            path
        }
        Err(_) => {
            println!("using default metric path: {}", SPU_MONITORING_UNIX_SOCKET);
            SPU_MONITORING_UNIX_SOCKET.to_owned()
        }
    };

    // check if file exists
    if let Ok(_metadata) = std::fs::metadata(&metric_out_path) {
        println!("metric file already exists, deleting: {}", metric_out_path);
        match std::fs::remove_file(&metric_out_path) {
            Ok(_) => {}
            Err(err) => {
                println!("error deleting metric file: {}", err);
                return Err(err);
            }
        }
    }

    let listener = UnixListener::bind(metric_out_path)?;
    let mut incoming = listener.incoming();
    println!("monitoring started");

    let metrics: &SpuMetrics = &ctx.metrics();
    while let Some(stream) = incoming.next().await {
        let mut stream = stream?;

        // println!("metrics: {:?}", metrics);
        let bytes = serde_json::to_vec_pretty(metrics)?;
        stream.write_all(&bytes).await?;
    }

    Ok(())
}

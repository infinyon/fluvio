use std::io::Error as IoError;

use async_net::unix::UnixListener;

use futures_util::{StreamExt, AsyncWriteExt};
use fluvio_types::defaults::SC_MONITORING_UNIX_SOCKET;
use fluvio_future::task::spawn;
use tracing::{error, info, debug};

use crate::core::{K8SharedContext, metrics::ScMetrics};

pub(crate) fn init_monitoring(ctx: K8SharedContext) {
    spawn(async move {
        if let Err(err) = start_monitoring(ctx).await {
            error!("error running monitoring: {}", err);
        }
    });
}

/// initialize if monitoring flag is set
async fn start_monitoring(ctx: K8SharedContext) -> Result<(), IoError> {
    let metric_out_path = match std::env::var("FLUVIO_METRIC_SC") {
        Ok(path) => {
            info!("using metric path: {}", path);
            path
        }
        Err(_) => {
            info!("using default metric path: {}", SC_MONITORING_UNIX_SOCKET);
            SC_MONITORING_UNIX_SOCKET.to_owned()
        }
    };

    loop {
        // check if file exists
        if let Ok(_metadata) = std::fs::metadata(&metric_out_path) {
            debug!("metric file already exists, deleting: {}", metric_out_path);
            match std::fs::remove_file(&metric_out_path) {
                Ok(_) => {}
                Err(err) => {
                    error!("error deleting metric file: {}", err);
                    return Err(err);
                }
            }
        }

        let listener = UnixListener::bind(&metric_out_path)?;
        let mut incoming = listener.incoming();
        info!("monitoring started");

        let metrics: &ScMetrics = &ctx.metrics();
        while let Some(stream) = incoming.next().await {
            let mut stream = match stream {
                Ok(stream) => stream,
                Err(err) => {
                    error!("error accepting connection: {}", err);
                    break;
                }
            };

            let bytes = serde_json::to_vec_pretty(metrics)?;
            stream.write_all(&bytes).await?;
        }

        info!("monitoring socket closed. Trying to reconnect in 5 seconds");
        fluvio_future::timer::sleep(std::time::Duration::from_secs(5)).await;
    }
}

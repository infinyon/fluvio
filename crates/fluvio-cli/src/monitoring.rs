pub(crate) use monitor_impl::init_monitoring;

use fluvio::metrics::ClientMetrics;

#[cfg(windows)]
mod monitor_impl {

    use std::sync::Arc;
    use super::ClientMetrics;

    pub(crate) fn init_monitoring(_metrics: Arc<ClientMetrics>) {}
}

#[cfg(unix)]
mod monitor_impl {

    use std::{io::Error as IoError, sync::Arc};

    use tracing::debug;

    use async_net::unix::UnixListener;
    use futures_util::{StreamExt, AsyncWriteExt};

    use fluvio_future::task::spawn;

    use super::ClientMetrics;

    pub(crate) fn init_monitoring(metrics: Arc<ClientMetrics>) {
        spawn(async move {
            if let Err(err) = start_monitoring(metrics).await {
                println!("error running monitoring: {err}");
            }
        });
    }

    /// initialize if monitoring flag is set
    async fn start_monitoring(metrics: Arc<ClientMetrics>) -> Result<(), IoError> {
        let metric_out_dir = match std::env::var("FLUVIO_METRIC_CLIENT_DIR") {
            Ok(path) => {
                println!("using metric dir: {path}");
                path
            }
            Err(_) => {
                debug!("no metric dir set, do noting");
                return Ok(());
            }
        };

        // create unique file name which is pid
        let pid = std::process::id();
        let metric_out_path = format!("{metric_out_dir}/fluvio-client-{pid}.sock");

        println!("using metric path: {metric_out_path}");

        // check if file exists
        if let Ok(_metadata) = std::fs::metadata(&metric_out_path) {
            println!("metric file already exists, deleting: {metric_out_path}");
            match std::fs::remove_file(&metric_out_path) {
                Ok(_) => {}
                Err(err) => {
                    println!("error deleting metric file: {err}");
                    return Err(err);
                }
            }
        }

        let listener = UnixListener::bind(metric_out_path)?;
        let mut incoming = listener.incoming();
        println!("monitoring started");

        while let Some(stream) = incoming.next().await {
            let mut stream = stream?;

            let bytes = serde_json::to_vec_pretty(metrics.as_ref())?;
            stream.write_all(&bytes).await?;
        }

        Ok(())
    }
}

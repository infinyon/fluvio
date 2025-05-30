use std::{io::Error as IoError, sync::Arc, collections::HashMap};

use futures_util::{AsyncWriteExt, StreamExt};

use fluvio::metrics::ClientMetrics;
use fluvio_future::task::spawn;
use fluvio_future::net::unix::UnixListener;
use tracing::{error, info, trace};
use serde::Serialize;
use fluvio_smartengine::metrics::SmartModuleChainMetrics;

const SOCKET_PATH: &str = "/tmp/fluvio-connector.sock";

#[derive(Debug, Serialize)]
pub struct ConnectorMetrics {
    #[serde(flatten)]
    fluvio_metrics: Arc<ClientMetrics>,
    // Added field to capture per-SmartModule metrics
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    smartmodule_metrics: HashMap<String, SmartModuleChainMetrics>,
}

impl Default for ConnectorMetrics {
    fn default() -> Self {
        Self {
            fluvio_metrics: Arc::new(ClientMetrics::new()),
            smartmodule_metrics: HashMap::new(),
        }
    }
}

impl ConnectorMetrics {
    pub fn new(fluvio_metrics: Arc<ClientMetrics>) -> Self {
        Self {
            fluvio_metrics,
            smartmodule_metrics: HashMap::new(),
        }
    }

    // Add method to update smartmodule metrics
    pub fn update_smartmodule_metrics(
        &mut self,
        smartmodule_name: &str,
        metrics: &SmartModuleChainMetrics,
    ) {
        if let Some(existing_metrics) = self.smartmodule_metrics.get_mut(smartmodule_name) {
            existing_metrics.append(metrics);
        } else {
            self.smartmodule_metrics
                .insert(smartmodule_name.to_string(), metrics.clone());
        }
    }

    // Get metrics for a specific smartmodule
    pub fn get_smartmodule_metrics(
        &self,
        smartmodule_name: &str,
    ) -> Option<&SmartModuleChainMetrics> {
        self.smartmodule_metrics.get(smartmodule_name)
    }

    // Get all smartmodule metrics
    pub fn smartmodule_metrics(&self) -> &HashMap<String, SmartModuleChainMetrics> {
        &self.smartmodule_metrics
    }
}

pub fn init_monitoring(metrics: Arc<ConnectorMetrics>) {
    spawn(async move {
        if let Err(err) = start_monitoring(metrics).await {
            error!("error running monitoring: {}", err);
        }
    });
}

/// initialize if monitoring flag is set
async fn start_monitoring(metrics: Arc<ConnectorMetrics>) -> Result<(), IoError> {
    let metric_out_path = match std::env::var("FLUVIO_METRIC_CONNECTOR") {
        Ok(path) => {
            info!("using metric path: {}", path);
            path
        }
        Err(_) => {
            info!("using default metric path: {}", SOCKET_PATH);
            SOCKET_PATH.to_owned()
        }
    };

    loop {
        // check if file exists
        if let Ok(_metadata) = std::fs::metadata(&metric_out_path) {
            info!("metric file already exists, deleting: {}", metric_out_path);
            match std::fs::remove_file(&metric_out_path) {
                Ok(_) => {}
                Err(err) => {
                    println!("error deleting metric file: {err}");
                    return Err(err);
                }
            }
        }

        let listener = UnixListener::bind(&metric_out_path)?;
        let mut incoming = listener.incoming();
        info!("monitoring started");

        while let Some(stream) = incoming.next().await {
            let mut stream = match stream {
                Ok(stream) => stream,
                Err(err) => {
                    error!("error accepting connection: {}", err);
                    break;
                }
            };

            trace!("metrics: {:?}", metrics);
            let bytes = serde_json::to_vec_pretty(metrics.as_ref())?;
            stream.write_all(&bytes).await?;
        }
        info!("monitoring socket closed. Trying to reconnect in 5 seconds");
        fluvio_future::timer::sleep(std::time::Duration::from_secs(5)).await;
    }
}

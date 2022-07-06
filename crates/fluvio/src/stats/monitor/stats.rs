use std::sync::Arc;
use std::time::Duration;
use crate::stats::{ClientStats, ClientStatsUpdate, ClientStatsDataCollect, ClientStatsHistogram};
use tracing::error;
use crate::FluvioError;

impl ClientStats {
    /// Run stats aggregation in the background
    pub fn start_stats_histogram(stats: Arc<ClientStats>) {
        if !stats.is_collect(ClientStatsDataCollect::None) {
            fluvio_future::task::spawn(async move {
                if stats_histogram_loop(stats).await.is_ok() {
                } else {
                    error!("There was a non-fatal error starting histogram loop");
                }
            });
        }
    }
}

async fn stats_histogram_loop(stats: Arc<ClientStats>) -> Result<(), FluvioError> {
    use tokio::select;

    let mut histogram = ClientStatsHistogram::new();
    let mut second_timer = Some(fluvio_future::timer::sleep(Duration::from_secs(1)));
    let mut second_marked = false;

    loop {
        select! {
            _ = async { second_timer.as_mut().expect("unexpected failure").await }, if second_timer.is_some() => {

                // Marking the second to establish window
                if !second_marked {
                    let _ = &histogram.mark_second();
                    second_marked = true;
                }
            }
            _ = async { stats.event_handler.listen_batch_event().await } => {
                let datapoint = stats.get_datapoint();

                let _ =  &histogram.record(datapoint);

                // Update the stats frame
                let updates = histogram.get_agg_stats_update()?;

                stats.update(ClientStatsUpdate { data: updates })?;
            }

        }
    }
}

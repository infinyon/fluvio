use std::sync::Arc;
use std::time::Duration;
use crate::stats::{
    ClientStats, ClientStatsUpdateBuilder, ClientStatsDataCollect, ClientStatsHistogram,
};
use tracing::error;
use crate::FluvioError;

impl ClientStats {
    /// Run stats aggregation in the background
    pub fn start_stats_histogram(stats: Arc<ClientStats>) {
        //println!("Maybe start histogram loop: {stats:#?}");
        if stats.is_collect(ClientStatsDataCollect::Data) {
            //println!("About to start histogram loop");
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
    let mut seen_batch = false;

    loop {
        select! {
            _ = async { second_timer.as_mut().expect("unexpected failure").await }, if second_timer.is_some() => {

                // Marking the second to establish window
                if !second_marked && seen_batch {
                    let _ = &histogram.tick();
                    second_marked = true;
                } else {

                    let _ = &histogram.tick();
                }

                second_timer = Some(fluvio_future::timer::sleep(Duration::from_secs(1)));
            }
            _ = async { stats.event_handler.listen_batch_event().await } => {
                if !seen_batch { seen_batch = true }
                let dataframe = stats.get_dataframe();

                let _ =  &histogram.record(dataframe);

                // Update the stats frame
                let updates = histogram.get_agg_stats_update()?;

                stats.update(ClientStatsUpdateBuilder { data: updates })?;
            }

        }
    }
}

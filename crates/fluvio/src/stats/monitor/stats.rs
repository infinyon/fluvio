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
    //let mut second_marked = false;

    loop {
        select! {
            _ = async { second_timer.as_mut().expect("unexpected failure").await }, if second_timer.is_some() => {

                // Fixme
                // This is too strict. We need to have a batch to send first otherwise the frame with be len 0
                // Marking the second to establish window
                if !second_marked {
                    let _ = &histogram.tick();
                    second_marked = true;
                } else {

                    let _ = &histogram.tick();
                }

                second_timer = Some(fluvio_future::timer::sleep(Duration::from_secs(1)));
            }
            _ = async { stats.event_handler.listen_batch_event().await } => {
                let dataframe = stats.get_dataframe();

                let _ =  &histogram.record(dataframe);

                // Update the stats frame
                let updates = histogram.get_agg_stats_update()?;

                stats.update(ClientStatsUpdateBuilder { data: updates })?;
            }

        }
    }
}

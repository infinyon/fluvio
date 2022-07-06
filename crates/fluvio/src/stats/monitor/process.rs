use std::sync::Arc;
use std::time::Duration;
use crate::stats::{
    ClientStats, ClientStatsDataCollect, ClientStatsUpdateBuilder, ClientStatsMetricRaw,
};
use tracing::{debug, error};
use crate::FluvioError;
use sysinfo::{self, SystemExt, ProcessExt};

impl ClientStats {
    /// Run system resource data sampling in the background
    pub fn start_system_monitor(stats: Arc<ClientStats>) {
        if stats.is_collect(ClientStatsDataCollect::System) {
            fluvio_future::task::spawn(async move {
                if system_resource_sampler(stats).await.is_ok() {
                } else {
                    error!("There was a non-fatal error gathering system stats");
                }
            });
        }
    }
}

/// Sample memory and cpu being used by the client
/// Refresh process resource monitor every second
async fn system_resource_sampler(stats: Arc<ClientStats>) -> Result<(), FluvioError> {
    let mut sysinfo = sysinfo::System::new();
    let pid = sysinfo::get_current_pid().map_err(|e| FluvioError::Other(e.to_string()))?;
    let cpu_cores = sysinfo
        .physical_core_count()
        .ok_or_else(|| FluvioError::Other("Unable to get number of CPU cores".to_string()))?
        as f32;

    // Warm up the resource probe
    sysinfo.refresh_process(pid);

    use tokio::select;
    const REFRESH_RATE_MILLIS: u64 = 1000;

    let mut system_poll_time = Some(fluvio_future::timer::sleep(Duration::from_millis(0)));

    loop {
        select! {
            _ = async { system_poll_time.as_mut().expect("unexpected failure").await }, if system_poll_time.is_some() => {

                let mut data_sample_update = ClientStatsUpdateBuilder::default();

                debug!("Updating Client resource usage");
                sysinfo.refresh_process(pid);

                let proc = sysinfo.process(pid).ok_or_else(|| FluvioError::Other(
                    "Unable to read current process".to_string(),
                ))?;

                // Memory usage is reported in kilobytes
                let mem_used_sample = proc.memory() + proc.virtual_memory();
                data_sample_update.push(ClientStatsMetricRaw::Mem(mem_used_sample));

                // Cpu usage in percentage, adjusted for the # of cores
                let cpu_used_percent = proc.cpu_usage() / cpu_cores;

                // Store cpu percentage w/ 3 decimal places
                let cpu_shift = cpu_used_percent * 1_000.0;

                data_sample_update.push(ClientStatsMetricRaw::Cpu(cpu_shift as u32));

                stats.update(data_sample_update)?;

                system_poll_time = Some(fluvio_future::timer::sleep(Duration::from_millis(REFRESH_RATE_MILLIS)));
            }
        }
    }
}

mod data_point;
mod update;
pub use data_point::ClientStatsDataPoint;
pub use update::ClientStatsUpdate;

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicI64, AtomicI32, Ordering};
use std::{
    time::{Duration, Instant},
};
use chrono::Utc;
use std::sync::Arc;
use sysinfo::{self, PidExt};
use tracing::{debug, error};

use crate::sockets::VersionedSerialSocket;
use dataplane::produce::{ProduceRequest, ProduceResponse};
use dataplane::record::RecordSet;
use dataplane::batch::RawRecords;

use crate::error::{Result, FluvioError};
use sysinfo::{SystemExt, ProcessExt};

/// Ordering used by `std::sync::atomic` operations
pub(crate) const STATS_MEM_ORDER: Ordering = Ordering::Relaxed;

/// Used for configuring the type of data to collect
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ClientStatsDataCollect {
    /// Collect all available stats
    All,
    /// Do not collect stats
    None,
    /// Collect measurements for throughput and latency
    Data,
    /// Collect measurements for CPU and memory usage
    System,
}

/// Main struct for recording client stats
#[derive(Debug)]
pub struct ClientStats {
    /// Start time when struct was created
    /// This is Unix Epoch time, in nanoseconds
    start_time: AtomicI64,
    /// PID of the client process
    pid: AtomicU32,
    /// Offset from last batch seen
    offset: AtomicI32,
    /// Last time any struct values were updated
    /// This is Unix Epoch time, in nanoseconds
    last_updated: AtomicI64,
    /// Time it took to complete last data transfer, in nanoseconds
    last_latency: AtomicU64,
    /// Data volume of last data transfer, in bytes
    last_bytes: AtomicU64,
    /// Total number of records processed since struct created
    num_records: AtomicU64,
    /// Data volume of all data transferred, in bytes
    total_bytes: AtomicU64,
    /// Last polled memory usage of client process, in kilobytes
    last_mem: AtomicU64,
    /// Last polled CPU usage, adjusted for host system's # of CPU cores
    /// Value is `u32` adjusted percentage, shifted 3 decimal places
    /// ex. 12.345%  =>  12345
    /// User of stats will need to account for this conversion
    last_cpu: AtomicU32,
    /// Configuration of what data client is to collect
    stats_collect: ClientStatsDataCollect,
}

/// Helper function. Get Unix Epoch time for Utc timezone, in nanoseconds
fn unix_timestamp_nanos() -> i64 {
    Utc::now().timestamp_nanos()
}

impl Default for ClientStats {
    fn default() -> Self {
        // Get pid
        let pid = if let Ok(pid) = sysinfo::get_current_pid() {
            pid.as_u32()
        } else {
            0
        };

        let unix_epoch = unix_timestamp_nanos();

        ClientStats {
            start_time: AtomicI64::new(unix_epoch),
            pid: AtomicU32::new(pid),
            offset: AtomicI32::new(0),
            last_updated: AtomicI64::new(unix_epoch),
            last_latency: AtomicU64::new(0),
            last_bytes: AtomicU64::new(0),
            num_records: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
            last_mem: AtomicU64::new(0),
            last_cpu: AtomicU32::new(0),
            stats_collect: ClientStatsDataCollect::None,
        }
    }
}

impl ClientStats {
    /// Create a new `ClientStats`. `stats_collect` selects the type of data to collect
    pub fn new(stats_collect: ClientStatsDataCollect) -> Self {
        Self {
            stats_collect,
            ..Default::default()
        }
    }

    /// Create a new `ClientStats` wrapped in `Arc<T>`
    pub fn new_shared(stats_collect: ClientStatsDataCollect) -> Arc<ClientStats> {
        Arc::new(Self::new(stats_collect))
    }

    /// Run system resource data sampling in the background
    pub fn start_system_monitor(stats: Arc<ClientStats>) {
        if stats.is_collect(ClientStatsDataCollect::System) {
            fluvio_future::task::spawn(async move {
                if Self::system_resource_sampler(stats).await.is_ok() {
                } else {
                    error!("There was a non-fatal error gathering system stats");
                }
            });
        }
    }

    /// Return true if `option` meets requirements to collect data type
    pub fn is_collect(&self, option: ClientStatsDataCollect) -> bool {
        self.stats_collect == ClientStatsDataCollect::All
            || (self.stats_collect != ClientStatsDataCollect::None)
                && (self.stats_collect == option)
    }

    /// Return the start time in nanoseconds
    pub fn start_time(&self) -> i64 {
        self.start_time.load(STATS_MEM_ORDER)
    }

    /// Return the last updated time in nanoseconds
    pub fn last_updated(&self) -> i64 {
        self.last_updated.load(STATS_MEM_ORDER)
    }

    /// Return configured collection option
    pub fn stats_collect(&self) -> ClientStatsDataCollect {
        self.stats_collect
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
        let mut data_sample_update = ClientStatsUpdate::default();

        loop {
            select! {
                _ = async { system_poll_time.as_mut().expect("unexpected failure").await }, if system_poll_time.is_some() => {
                    debug!("Updating Client resource usage");
                    sysinfo.refresh_process(pid);

                    let proc = sysinfo.process(pid).ok_or_else(|| FluvioError::Other(
                        "Unable to read current process".to_string(),
                    ))?;

                    // Memory usage is reported in kilobytes
                    let mem_used_sample = proc.memory() + proc.virtual_memory();
                    data_sample_update.set_mem(Some(mem_used_sample));

                    // Cpu usage in percentage, adjusted for the # of cores
                    let cpu_used_percent = proc.cpu_usage() / cpu_cores;

                    // Store cpu percentage w/ 3 decimal places
                    let cpu_shift = cpu_used_percent * 1_000.0;

                    data_sample_update.set_cpu(Some(cpu_shift as u32));

                    stats.update(data_sample_update);

                    system_poll_time = Some(fluvio_future::timer::sleep(Duration::from_millis(REFRESH_RATE_MILLIS)));
                }
            }
        }
    }

    /// Update the instance with values from `ClientStatsUpdate`
    pub fn update(&self, update: ClientStatsUpdate) {
        if let Some(bytes) = update.bytes() {
            self.last_bytes.fetch_add(bytes, STATS_MEM_ORDER);
            self.total_bytes.fetch_add(bytes, STATS_MEM_ORDER);
        }

        if let Some(cpu) = update.cpu() {
            self.last_cpu.store(cpu, STATS_MEM_ORDER);
        }

        if let Some(latency) = update.latency() {
            self.last_latency.store(latency, STATS_MEM_ORDER);
        }

        if let Some(mem) = update.mem() {
            self.last_mem.store(mem, STATS_MEM_ORDER);
        }

        if let Some(records) = update.records() {
            self.num_records.fetch_add(records, STATS_MEM_ORDER);
        }

        if let Some(offset) = update.offset() {
            self.offset.store(offset, STATS_MEM_ORDER);
        }

        let t = unix_timestamp_nanos();
        self.last_updated.store(t, STATS_MEM_ORDER);
    }

    /// Return the current `ClientStats` as `ClientStatsDataPoint`
    pub fn get_datapoint(&self) -> ClientStatsDataPoint {
        self.into()
    }

    /// Return the pid of the client
    pub fn pid(&self) -> u32 {
        self.pid.load(STATS_MEM_ORDER)
    }

    /// Returns the offset last seen
    pub fn offset(&self) -> i32 {
        self.offset.load(STATS_MEM_ORDER)
    }

    /// Returns the last data transfer size in bytes
    pub fn last_bytes(&self) -> u64 {
        self.last_bytes.load(STATS_MEM_ORDER)
    }

    /// Returns the accumulated data transfer size in bytes
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes.load(STATS_MEM_ORDER)
    }

    /// Returns the latency of last transfer in nanoseconds
    pub fn last_latency(&self) -> u64 {
        self.last_latency.load(STATS_MEM_ORDER)
    }

    /// Returns the last memory usage sample in kilobytes
    pub fn memory(&self) -> u64 {
        self.last_mem.load(STATS_MEM_ORDER)
    }

    /// Returns the last cpu usage sample as an integer
    /// Representing a percentage (%) with 3 decimal places
    /// Divide by 1_000.0 convert to back to percentage
    pub fn cpu(&self) -> u64 {
        self.last_cpu.load(STATS_MEM_ORDER).into()
    }

    /// Returns the current uptime of the client in nanoseconds
    pub fn uptime(&self) -> i64 {
        unix_timestamp_nanos() - self.start_time.load(STATS_MEM_ORDER)
    }

    /// Returns the number of records transferred
    pub fn records(&self) -> u64 {
        self.num_records.load(STATS_MEM_ORDER)
    }

    /// Record the latency of a producer request
    pub async fn send_and_measure_latency(
        &self,
        socket: &VersionedSerialSocket,
        request: ProduceRequest<RecordSet<RawRecords>>,
    ) -> Result<(ProduceResponse, ClientStatsUpdate)> {
        let send_start_time = Instant::now();

        let response = socket.send_receive(request).await?;

        let send_latency = Some(send_start_time.elapsed().as_nanos() as u64);

        let stats_update = ClientStatsUpdate::new().set_latency(send_latency);

        Ok((response, stats_update))
    }
}

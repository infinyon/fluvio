use std::{
    time::{Duration, Instant},
};
use chrono::{DateTime, Utc};
use std::sync::Arc;
use async_lock::RwLock;
use sysinfo::{self, PidExt};
use tracing::{debug, error};

use quantities::{prelude::*, AMNT_ZERO, datathroughput::DataThroughput};
use quantities::datavolume::{DataVolume, BYTE, KILOBYTE};
use quantities::duration::{Duration as QuantDuration, SECOND};

use crate::FluvioError;
use sysinfo::{SystemExt, ProcessExt};

// This struct is heavily utilizing
// `quantities` crate for managing unit conversions
#[derive(Debug, Clone)]
pub struct ClientStats {
    start_time: Instant,
    pid: u64,
    last_updated: DateTime<Utc>,
    last_latency: QuantDuration,
    last_bytes: DataVolume,
    num_records: u64,
    total_bytes: DataVolume,
    last_mem: DataVolume,
    last_cpu: f32,
}

impl Default for ClientStats {
    fn default() -> Self {
        // Get pid
        let pid = if let Ok(pid) = sysinfo::get_current_pid() {
            pid.as_u32()
        } else {
            0
        };

        ClientStats {
            start_time: Instant::now(),
            pid: pid.into(),
            last_updated: Utc::now(),
            last_latency: AMNT_ZERO * SECOND,
            last_bytes: AMNT_ZERO * BYTE,
            num_records: 0,
            total_bytes: AMNT_ZERO * BYTE,
            last_mem: AMNT_ZERO * BYTE,
            last_cpu: 0.0,
        }
    }
}

impl ClientStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_shared() -> Arc<RwLock<Self>> {
        Arc::new(RwLock::new(Self::new()))
    }

    pub fn start(shared: Arc<RwLock<Self>>) {
        fluvio_future::task::spawn(async move {
            if Self::run(shared).await.is_ok() {
            } else {
                error!("There was a non-fatal error gathering system stats");
            }
        });
    }

    async fn run(shared: Arc<RwLock<Self>>) -> Result<(), FluvioError> {
        let mut sysinfo = sysinfo::System::new();
        let pid = sysinfo::get_current_pid().map_err(|e| FluvioError::Other(e.to_string()))?;
        let cpu_cores = sysinfo
            .physical_core_count()
            .ok_or_else(|| FluvioError::Other("Unable to get number of CPU cores".to_string()))?
            as f32;

        use tokio::select;
        const REFRESH_RATE_MILLIS: u64 = 100;

        let mut system_poll_time = Some(fluvio_future::timer::sleep(Duration::from_millis(0)));

        loop {
            select! {


                _ = async { system_poll_time.as_mut().expect("unexpected failure").await }, if system_poll_time.is_some() => {
                    debug!("Updating Client resource usage");
                    sysinfo.refresh_process(pid);

                    let proc = sysinfo.process(pid).ok_or_else(|| FluvioError::Other(
                        "Unable to read current process".to_string(),
                    ))?;

                    // Take a cpu/memory sample here
                    let mem_used_sample = proc.memory() + proc.virtual_memory();

                    //debug!("memory {:#?}", mem_used_sample);
                    let scratch = mem_used_sample as f64;
                    let mem_used = Some(scratch * KILOBYTE);

                    let cpu_used_sample = proc.cpu_usage()  ;
                    //debug!("cpu {:#?}", cpu_used_sample);

                    let cpu_used = Some(cpu_used_sample / cpu_cores);

                    let stats_update = ClientStatsUpdate::new()
                        .cpu(cpu_used)
                        .mem(mem_used);

                    let mut stats_handle = shared.write().await;
                    stats_handle.update(stats_update);
                    drop(stats_handle);

                    system_poll_time = Some(fluvio_future::timer::sleep(Duration::from_millis(REFRESH_RATE_MILLIS)));
                }
            }
        }
    }

    pub fn update(&mut self, update: ClientStatsUpdate) {
        if let Some(bytes) = update.bytes {
            self.last_bytes = bytes;
            self.total_bytes = self.total_bytes + bytes;
        }

        if let Some(cpu) = update.cpu {
            self.last_cpu = cpu;
        }

        if let Some(latency) = update.latency {
            self.last_latency = latency;
        }

        if let Some(mem) = update.mem {
            self.last_mem = mem;
        }

        if let Some(records) = update.records {
            self.num_records += records;
        }

        self.last_updated = Utc::now();
    }

    pub fn snapshot(&self) -> ClientStats {
        self.clone()
    }

    // TODO: Try to identify reasonable units, but offer ability to control unit
    /// Return the throughput of the last batch transfer in units TBD
    pub fn throughput(&self) -> DataThroughput {
        self.last_bytes / self.last_latency
    }

    /// Return the pid of the client
    pub fn pid(&self) -> u64 {
        self.pid
    }

    // TODO: Collect the topic, or otherwise figure out how we can return that value
    pub fn topic(&self) -> String {
        String::new()
    }

    // TODO: Same deal here, collect the offset or query for it
    pub fn offset(&self) -> u64 {
        0
    }

    /// Returns the last data transfer size in bytes
    pub fn last_bytes(&self) -> u64 {
        self.last_bytes.amount() as u64
    }

    /// Returns the accumulated data transfer size in bytes
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes.amount() as u64
    }

    /// Returns the latency of last transfer -> Duration {
    pub fn last_latency(&self) -> QuantDuration {
        self.last_latency
    }

    /// Returns the last memory usage sample in kilobytes
    pub fn memory(&self) -> u64 {
        self.last_mem.amount() as u64
    }

    /// Returns the last cpu usage sample as a percentage (%)
    pub fn cpu(&self) -> f32 {
        self.last_cpu
    }

    /// Returns the current uptime of the client
    pub fn uptime(&self) -> QuantDuration {
        let uptime = self.start_time.elapsed();
        let scratch: AmountT = uptime.as_secs_f64();
        scratch * SECOND
    }

    /// Returns the number of records transferred
    pub fn records(&self) -> u64 {
        self.num_records
    }

    pub fn print_current_stats(&self) -> String {
        format!(
            "throughput: {:.2}, latency: {}, memory: {}, CPU: {:.2}",
            self.throughput(),
            self.last_latency(),
            self.memory(),
            self.cpu()
        )
    }

    pub fn print_summary_stats(&self) -> String {
        format!(
            "uptime: {}, transferred: {} records, total data: {}",
            self.uptime(),
            self.records(),
            self.total_bytes(),
        )
    }
}

//impl fmt::Display for ClientStats {
//    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//        let throughput = self.throughput();
//
//        let report = json!({
//            "pid" : self.pid(),
//            "topic": self.topic(),
//            "offset" : self.offset(),
//            "last_transfer" : self.last_bytes().to_string(),
//            "total_transfer" : self.total_bytes().to_string(),
//            "throughput" : format!("{:.2} {}", throughput.amount(), throughput.unit()),
//            "memory" : self.memory().to_string(),
//            "cpu" : format!("{:.2}", self.cpu()),
//        });
//
//        write!(f, "{}", report.to_string())
//    }
//}

#[derive(Debug, Clone, Default)]
pub struct ClientStatsUpdate {
    bytes: Option<DataVolume>,
    cpu: Option<f32>,
    latency: Option<QuantDuration>,
    mem: Option<DataVolume>,
    records: Option<u64>,
    // offset?
}

impl ClientStatsUpdate {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn latency(&self, latency: Option<QuantDuration>) -> Self {
        let mut new = self.clone();
        new.latency = latency;
        new
    }

    pub fn bytes(&self, bytes: Option<DataVolume>) -> Self {
        let mut new = self.clone();
        new.bytes = bytes;
        new
    }

    pub fn records(&self, records: Option<u64>) -> Self {
        let mut new = self.clone();
        new.records = records;
        new
    }

    pub fn mem(&self, mem: Option<DataVolume>) -> Self {
        let mut new = self.clone();
        new.mem = mem;
        new
    }

    pub fn cpu(&self, cpu: Option<f32>) -> Self {
        let mut new = self.clone();
        new.cpu = cpu;
        new
    }
}

//impl DerefMut for Arc<ClientStats> {
//    fn deref_mut(&mut self) -> &mut Self::Target {
//        &mut self
//    }
//}

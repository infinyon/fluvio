use std::{
    time::{Duration, Instant},
};
use chrono::{DateTime, Utc};
use std::sync::Arc;
use sysinfo::{self, PidExt};
use tracing::{debug, error};

use quantities::{prelude::*, AMNT_ZERO, AMNT_ONE};
use quantities::datathroughput::{
    DataThroughput, TERABYTE_PER_SECOND, GIGABYTE_PER_SECOND, MEGABYTE_PER_SECOND,
    KILOBYTE_PER_SECOND,
};
use quantities::datavolume::{DataVolume, BYTE, KILOBYTE, MEGABYTE, GIGABYTE, TERABYTE};
use quantities::duration::{
    Duration as QuantDuration, MINUTE, SECOND, MILLISECOND, MICROSECOND, NANOSECOND,
};

use crate::FluvioError;
use sysinfo::{SystemExt, ProcessExt};
use arc_swap::ArcSwapOption;

pub type SharedClientStats = Arc<ArcSwapOption<ClientStats>>;

// This struct is heavily utilizing
// `quantities` crate for managing human readable unit conversions
#[derive(Debug, Clone, Copy)]
pub struct ClientStats {
    start_time: Instant,
    pid: u64,
    offset: i32,
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
            offset: 0,
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

    pub fn new_shared() -> SharedClientStats {
        Arc::new(ArcSwapOption::new(Some(Arc::new(Self::new()))))
    }

    /// Run system resource data sampling in the background
    pub fn start(shared: SharedClientStats) {
        fluvio_future::task::spawn(async move {
            if Self::system_resource_sampler(shared).await.is_ok() {
            } else {
                error!("There was a non-fatal error gathering system stats");
            }
        });
    }

    /// Sample memory and cpu being used by the client
    async fn system_resource_sampler(shared: SharedClientStats) -> Result<(), FluvioError> {
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
                    debug!("Updating Client resource usage");
                    sysinfo.refresh_process(pid);

                    let proc = sysinfo.process(pid).ok_or_else(|| FluvioError::Other(
                        "Unable to read current process".to_string(),
                    ))?;

                    // Take a cpu/memory sample here
                    let mem_used_sample = proc.memory() + proc.virtual_memory();

                    //debug!("memory {:#?}", mem_used_sample);
                    #[cfg(not(target_arch = "wasm32"))]
                    let scratch = mem_used_sample as f64;
                    #[cfg(target_arch = "wasm32")]
                    let scratch = mem_used_sample as f32;

                    let mem_used = Some(scratch * KILOBYTE);

                    let cpu_used_sample = proc.cpu_usage()  ;
                    //debug!("cpu {:#?}", cpu_used_sample);

                    let cpu_used = Some(cpu_used_sample / cpu_cores);

                    shared.rcu(|inner| {
                        if let Some(s) = inner.clone() {
                            let mut stats = *s;

                            stats.update(
                                ClientStatsUpdate::new()
                                .cpu(cpu_used)
                                .mem(mem_used)
                            );

                            Some(Arc::new(stats))
                        } else {
                            None
                        }
                    });

                    system_poll_time = Some(fluvio_future::timer::sleep(Duration::from_millis(REFRESH_RATE_MILLIS)));
                }
            }
        }
    }

    /// Update the instance with values from `ClientStatsUpdate`
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

        if let Some(offset) = update.offset {
            self.offset = offset;
        }

        self.last_updated = Utc::now();
    }

    /// Returns the `ClientStats` with updates applied without modifying the underlying struct
    pub fn update_dry_run(&self, update: ClientStatsUpdate) -> ClientStats {
        let mut new = *self;
        new.update(update);
        new
    }

    /// Return a clone of the current `ClientStats`
    pub fn snapshot(&self) -> ClientStats {
        *self
    }

    /// Return the throughput of the last batch transfer
    pub fn throughput(&self) -> DataThroughput {
        Self::covert_to_largest_throughput_unit(self.last_bytes() / self.last_latency())
    }

    /// Return the throughput of the session
    pub fn total_throughput(&self) -> DataThroughput {
        Self::covert_to_largest_throughput_unit(self.total_bytes() / self.uptime())
    }

    /// Return the pid of the client
    pub fn pid(&self) -> u64 {
        self.pid
    }

    /// Returns the offset last seen
    pub fn offset(&self) -> i32 {
        self.offset
    }

    /// Returns the last data transfer size in bytes
    pub fn last_bytes(&self) -> DataVolume {
        Self::convert_to_largest_data_unit(self.last_bytes)
    }

    /// Returns the accumulated data transfer size in bytes
    pub fn total_bytes(&self) -> DataVolume {
        Self::convert_to_largest_data_unit(self.total_bytes)
    }

    /// Returns the latency of last transfer -> Duration {
    pub fn last_latency(&self) -> QuantDuration {
        Self::convert_to_largest_time_unit(self.last_latency)
    }

    /// Returns the last memory usage sample
    pub fn memory(&self) -> DataVolume {
        // We know this always starts in kilobytes
        Self::convert_to_largest_data_unit(self.last_mem)
    }

    /// Returns the last cpu usage sample as a percentage (%)
    pub fn cpu(&self) -> f32 {
        self.last_cpu
    }

    /// Returns the current uptime of the client
    pub fn uptime(&self) -> QuantDuration {
        let uptime = self.start_time.elapsed();

        #[cfg(not(target_arch = "wasm32"))]
        let scratch: AmountT = uptime.as_secs_f64();
        #[cfg(target_arch = "wasm32")]
        let scratch: AmountT = uptime.as_secs_f32();

        Self::convert_to_largest_time_unit(scratch * SECOND)
    }

    /// Returns the number of records transferred
    pub fn records(&self) -> u64 {
        self.num_records
    }

    fn convert_to_largest_time_unit(ref_value: QuantDuration) -> QuantDuration {
        let convert_unit = if ref_value > (AMNT_ONE * MINUTE) {
            Some(MINUTE)
        } else if ref_value > (AMNT_ONE * SECOND) {
            Some(SECOND)
        } else if ref_value > (AMNT_ONE * MILLISECOND) {
            Some(MILLISECOND)
        } else if ref_value > (AMNT_ONE * MICROSECOND) {
            Some(MICROSECOND)
        } else if ref_value > (AMNT_ONE * NANOSECOND) {
            Some(NANOSECOND)
        } else {
            None
        };

        if let Some(bigger_unit) = convert_unit {
            ref_value.convert(bigger_unit)
        } else {
            ref_value
        }
    }

    fn convert_to_largest_data_unit(ref_value: DataVolume) -> DataVolume {
        let convert_unit = if ref_value > (AMNT_ONE * TERABYTE) {
            Some(TERABYTE)
        } else if ref_value > (AMNT_ONE * GIGABYTE) {
            Some(GIGABYTE)
        } else if ref_value > (AMNT_ONE * MEGABYTE) {
            Some(MEGABYTE)
        } else if ref_value > (AMNT_ONE * KILOBYTE) {
            Some(KILOBYTE)
        } else if ref_value > (AMNT_ONE * BYTE) {
            Some(BYTE)
        } else {
            None
        };

        if let Some(bigger_unit) = convert_unit {
            ref_value.convert(bigger_unit)
        } else {
            ref_value
        }
    }

    fn covert_to_largest_throughput_unit(ref_value: DataThroughput) -> DataThroughput {
        let convert_unit = if ref_value > (AMNT_ONE * TERABYTE_PER_SECOND) {
            Some(TERABYTE_PER_SECOND)
        } else if ref_value > (AMNT_ONE * GIGABYTE_PER_SECOND) {
            Some(GIGABYTE_PER_SECOND)
        } else if ref_value > (AMNT_ONE * MEGABYTE_PER_SECOND) {
            Some(MEGABYTE_PER_SECOND)
        } else if ref_value > (AMNT_ONE * KILOBYTE_PER_SECOND) {
            Some(KILOBYTE_PER_SECOND)
        } else {
            None
        };

        if let Some(bigger_unit) = convert_unit {
            ref_value.convert(bigger_unit)
        } else {
            ref_value
        }
    }
}

/// Update builder for `ClientStats`
#[derive(Debug, Clone, Default, Copy)]
pub struct ClientStatsUpdate {
    bytes: Option<DataVolume>,
    cpu: Option<f32>,
    latency: Option<QuantDuration>,
    mem: Option<DataVolume>,
    records: Option<u64>,
    offset: Option<i32>,
}

impl ClientStatsUpdate {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn latency(&self, latency: Option<QuantDuration>) -> Self {
        let mut new = *self;
        new.latency = latency;
        new
    }

    pub fn bytes(&self, bytes: Option<DataVolume>) -> Self {
        let mut new = *self;
        new.bytes = bytes;
        new
    }

    pub fn records(&self, records: Option<u64>) -> Self {
        let mut new = *self;
        new.records = records;
        new
    }

    pub fn mem(&self, mem: Option<DataVolume>) -> Self {
        let mut new = *self;
        new.mem = mem;
        new
    }

    pub fn cpu(&self, cpu: Option<f32>) -> Self {
        let mut new = *self;
        new.cpu = cpu;
        new
    }

    pub fn offset(&self, offset: Option<i32>) -> Self {
        let mut new = *self;
        new.offset = offset;
        new
    }
}

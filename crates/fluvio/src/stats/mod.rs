mod data_point;
mod update;
mod histogram;
mod event;
pub use event::ClientStatsEvent;
mod monitor;
pub use histogram::ClientStatsHistogram;
pub use data_point::{ClientStatsDataPoint, ClientStatsMetric};
use serde::{Serialize, Deserialize};
pub use update::ClientStatsUpdate;
use strum::{Display, EnumIter, IntoEnumIterator};

use std::fmt::Debug;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicI64, AtomicI32, Ordering};
use std::time::Instant;
use chrono::Utc;
use std::sync::Arc;
use sysinfo::{self, PidExt};

use crate::sockets::VersionedSerialSocket;
use dataplane::produce::{ProduceRequest, ProduceResponse};
use dataplane::record::RecordSet;
use dataplane::batch::RawRecords;

use crate::error::{Result, FluvioError};

/// Ordering used by `std::sync::atomic` operations
pub(crate) const STATS_MEM_ORDER: Ordering = Ordering::Relaxed;

/// Used for configuring the type of data to collect
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
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

impl Default for ClientStatsDataCollect {
    fn default() -> Self {
        ClientStatsDataCollect::None
    }
}

impl ClientStatsDataCollect {
    pub fn to_metrics(&self) -> Vec<ClientStatsMetric> {
        match self {
            Self::All => ClientStatsMetric::iter().collect(),
            Self::None => {
                vec![]
            }
            // TODO: Add standard list of metrics for data mode
            Self::Data => {
                vec![]
            }
            // TODO: Add standard list of metrics for system mode
            Self::System => {
                vec![]
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Display, EnumIter)]
pub enum ClientStatsMetricRaw {
    #[strum(serialize = "start_time_ns")]
    StartTime(i64),
    #[strum(serialize = "uptime_ns")]
    Uptime(i64),
    #[strum(serialize = "pid")]
    Pid(u32),
    #[strum(serialize = "offset")]
    Offset(i32),
    #[strum(serialize = "last_batches")]
    LastBatches(u64),
    #[strum(serialize = "last_bytes")]
    LastBytes(u64),
    #[strum(serialize = "last_latency_ns")]
    LastLatency(u64),
    #[strum(serialize = "last_records")]
    LastRecords(u64),
    #[strum(serialize = "last_throughput_byte_p_ns")]
    LastThroughput(u64),
    #[strum(serialize = "last_updated_ns")]
    LastUpdated(i64),
    #[strum(serialize = "batches")]
    Batches(u64),
    #[strum(serialize = "bytes")]
    Bytes(u64),
    #[strum(serialize = "cpu_pct_x_1000")]
    Cpu(u32),
    #[strum(serialize = "mem_kb")]
    Mem(u64),
    #[strum(serialize = "latency_ns")]
    Latency(u64),
    #[strum(serialize = "records")]
    Records(u64),
    #[strum(serialize = "throughput_byte_p_ns")]
    Throughput(u64),
    #[strum(serialize = "batches_p_sec")]
    SecondBatches(u64),
    #[strum(serialize = "latency_p_sec")]
    SecondLatency(u64),
    #[strum(serialize = "records_p_sec")]
    SecondRecords(u64),
    #[strum(serialize = "throughput_bytes_p_sec")]
    SecondThroughput(u64),
    #[strum(serialize = "avg_latency_ns_p_sec")]
    SecondMeanLatency(u64),
    #[strum(serialize = "avg_throughput_byte_p_sec")]
    SecondMeanThroughput(u64),
    #[strum(serialize = "max_throughput_byte_p_sec")]
    MaxThroughput(u64),
    #[strum(serialize = "mean_throughput_byte_p_sec")]
    MeanThroughput(u64),
    #[strum(serialize = "mean_latency_ns")]
    MeanLatency(u64),
    #[strum(serialize = "std_dev_latency_ns")]
    StdDevLatency(u64),
    #[strum(serialize = "p50_latency_ns")]
    P50Latency(u64),
    #[strum(serialize = "p90_latency_ns")]
    P90Latency(u64),
    #[strum(serialize = "p99_latency_ns")]
    P99Latency(u64),
    #[strum(serialize = "p999_latency_ns")]
    P999Latency(u64),
}

impl ClientStatsMetricRaw {
    pub fn value_to_string(&self) -> String {
        match self {
            Self::StartTime(n) => n.to_string(),
            Self::Uptime(n) => n.to_string(),
            Self::Pid(n) => n.to_string(),
            Self::Offset(n) => n.to_string(),
            Self::LastBatches(n) => n.to_string(),
            Self::LastBytes(n) => n.to_string(),
            Self::LastLatency(n) => n.to_string(),
            Self::LastRecords(n) => n.to_string(),
            Self::LastThroughput(n) => n.to_string(),
            Self::LastUpdated(n) => n.to_string(),
            Self::Batches(n) => n.to_string(),
            Self::Bytes(n) => n.to_string(),
            Self::Cpu(n) => n.to_string(),
            Self::Mem(n) => n.to_string(),
            Self::Latency(n) => n.to_string(),
            Self::Records(n) => n.to_string(),
            Self::Throughput(n) => n.to_string(),
            Self::SecondBatches(n) => n.to_string(),
            Self::SecondLatency(n) => n.to_string(),
            Self::SecondRecords(n) => n.to_string(),
            Self::SecondThroughput(n) => n.to_string(),
            Self::SecondMeanLatency(n) => n.to_string(),
            Self::SecondMeanThroughput(n) => n.to_string(),
            Self::MaxThroughput(n) => n.to_string(),
            Self::MeanThroughput(n) => n.to_string(),
            Self::MeanLatency(n) => n.to_string(),
            Self::StdDevLatency(n) => n.to_string(),
            Self::P50Latency(n) => n.to_string(),
            Self::P90Latency(n) => n.to_string(),
            Self::P99Latency(n) => n.to_string(),
            Self::P999Latency(n) => n.to_string(),
        }
    }

    pub fn as_u32(&self) -> u32 {
        match self {
            Self::StartTime(n) => *n as u32,
            Self::Uptime(n) => *n as u32,
            Self::Pid(n) => *n as u32,
            Self::Offset(n) => *n as u32,
            Self::LastBatches(n) => *n as u32,
            Self::LastBytes(n) => *n as u32,
            Self::LastLatency(n) => *n as u32,
            Self::LastRecords(n) => *n as u32,
            Self::LastThroughput(n) => *n as u32,
            Self::LastUpdated(n) => *n as u32,
            Self::Batches(n) => *n as u32,
            Self::Bytes(n) => *n as u32,
            Self::Cpu(n) => *n as u32,
            Self::Mem(n) => *n as u32,
            Self::Latency(n) => *n as u32,
            Self::Records(n) => *n as u32,
            Self::Throughput(n) => *n as u32,
            Self::SecondBatches(n) => *n as u32,
            Self::SecondLatency(n) => *n as u32,
            Self::SecondRecords(n) => *n as u32,
            Self::SecondThroughput(n) => *n as u32,
            Self::SecondMeanLatency(n) => *n as u32,
            Self::SecondMeanThroughput(n) => *n as u32,
            Self::MaxThroughput(n) => *n as u32,
            Self::MeanThroughput(n) => *n as u32,
            Self::MeanLatency(n) => *n as u32,
            Self::StdDevLatency(n) => *n as u32,
            Self::P50Latency(n) => *n as u32,
            Self::P90Latency(n) => *n as u32,
            Self::P99Latency(n) => *n as u32,
            Self::P999Latency(n) => *n as u32,
        }
    }

    pub fn as_u64(&self) -> u64 {
        match self {
            Self::StartTime(n) => *n as u64,
            Self::Uptime(n) => *n as u64,
            Self::Pid(n) => *n as u64,
            Self::Offset(n) => *n as u64,
            Self::LastBatches(n) => *n as u64,
            Self::LastBytes(n) => *n as u64,
            Self::LastLatency(n) => *n as u64,
            Self::LastRecords(n) => *n as u64,
            Self::LastThroughput(n) => *n as u64,
            Self::LastUpdated(n) => *n as u64,
            Self::Batches(n) => *n as u64,
            Self::Bytes(n) => *n as u64,
            Self::Cpu(n) => *n as u64,
            Self::Mem(n) => *n as u64,
            Self::Latency(n) => *n as u64,
            Self::Records(n) => *n as u64,
            Self::Throughput(n) => *n as u64,
            Self::SecondBatches(n) => *n as u64,
            Self::SecondLatency(n) => *n as u64,
            Self::SecondRecords(n) => *n as u64,
            Self::SecondThroughput(n) => *n as u64,
            Self::SecondMeanLatency(n) => *n as u64,
            Self::SecondMeanThroughput(n) => *n as u64,
            Self::MaxThroughput(n) => *n as u64,
            Self::MeanThroughput(n) => *n as u64,
            Self::MeanLatency(n) => *n as u64,
            Self::StdDevLatency(n) => *n as u64,
            Self::P50Latency(n) => *n as u64,
            Self::P90Latency(n) => *n as u64,
            Self::P99Latency(n) => *n as u64,
            Self::P999Latency(n) => *n as u64,
        }
    }

    pub fn as_i64(&self) -> i64 {
        match self {
            Self::StartTime(n) => *n as i64,
            Self::Uptime(n) => *n as i64,
            Self::Pid(n) => *n as i64,
            Self::Offset(n) => *n as i64,
            Self::LastBatches(n) => *n as i64,
            Self::LastBytes(n) => *n as i64,
            Self::LastLatency(n) => *n as i64,
            Self::LastRecords(n) => *n as i64,
            Self::LastThroughput(n) => *n as i64,
            Self::LastUpdated(n) => *n as i64,
            Self::Batches(n) => *n as i64,
            Self::Bytes(n) => *n as i64,
            Self::Cpu(n) => *n as i64,
            Self::Mem(n) => *n as i64,
            Self::Latency(n) => *n as i64,
            Self::Records(n) => *n as i64,
            Self::Throughput(n) => *n as i64,
            Self::SecondBatches(n) => *n as i64,
            Self::SecondLatency(n) => *n as i64,
            Self::SecondRecords(n) => *n as i64,
            Self::SecondThroughput(n) => *n as i64,
            Self::SecondMeanLatency(n) => *n as i64,
            Self::SecondMeanThroughput(n) => *n as i64,
            Self::MaxThroughput(n) => *n as i64,
            Self::MeanThroughput(n) => *n as i64,
            Self::MeanLatency(n) => *n as i64,
            Self::StdDevLatency(n) => *n as i64,
            Self::P50Latency(n) => *n as i64,
            Self::P90Latency(n) => *n as i64,
            Self::P99Latency(n) => *n as i64,
            Self::P999Latency(n) => *n as i64,
        }
    }
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
    /// The number of batches process in last transfer
    last_batches: AtomicU64,
    /// Data volume of last data transfer, in bytes
    last_bytes: AtomicU64,
    /// Time it took to complete last data transfer, in nanoseconds
    last_latency: AtomicU64,
    /// The number of records in the last transfer
    last_records: AtomicU64,
    /// Throughput the last transfer, in bytes per nanosecond
    last_throughput: AtomicU64,
    /// Last time any struct values were updated
    /// This is Unix Epoch time, in nanoseconds
    last_updated: AtomicI64,
    /// Total number of batches processed
    batches: AtomicU64,
    /// Data volume of all data transferred, in bytes
    bytes: AtomicU64,
    /// Last polled CPU usage, adjusted for host system's # of CPU cores
    /// Value is `u32` adjusted percentage, shifted 3 decimal places
    /// ex. 12.345%  =>  12345
    /// User of stats will need to account for this conversion
    cpu: AtomicU32,
    /// Last polled memory usage of client process, in kilobytes
    mem: AtomicU64,
    /// Total time spent waiting for data transfer, in nanoseconds
    latency: AtomicU64,
    /// Total number of records processed
    records: AtomicU64,
    /// Batches per second, per one second sliding window
    second_batches: AtomicU64,
    /// Latency per second, per one second sliding window
    second_latency: AtomicU64,
    /// Records per second, per one second sliding window
    second_records: AtomicU64,
    /// Throughput in bytes per second, per one second sliding window
    second_throughput: AtomicU64,
    /// Mean Latency per second, per one second sliding window
    second_mean_latency: AtomicU64,
    /// Mean Throughput per second, per one second sliding window
    second_mean_throughput: AtomicU64,
    /// Maximum throughput recorded
    max_throughput: AtomicU64,
    /// Mean Throughput
    mean_throughput: AtomicU64,
    /// Mean latency
    mean_latency: AtomicU64,
    /// Standard deviation latency
    std_dev_latency: AtomicU64,
    /// P50 latency
    p50_latency: AtomicU64,
    /// P90 latency
    p90_latency: AtomicU64,
    /// P99 latency
    p99_latency: AtomicU64,
    /// P999 latency
    p999_latency: AtomicU64,
    event_handler: Arc<ClientStatsEvent>,
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
            last_batches: AtomicU64::new(0),
            last_bytes: AtomicU64::new(0),
            last_latency: AtomicU64::new(0),
            last_records: AtomicU64::new(0),
            last_throughput: AtomicU64::new(0),
            last_updated: AtomicI64::new(unix_epoch),
            batches: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            cpu: AtomicU32::new(0),
            mem: AtomicU64::new(0),
            latency: AtomicU64::new(0),
            records: AtomicU64::new(0),
            second_batches: AtomicU64::new(0),
            second_latency: AtomicU64::new(0),
            second_records: AtomicU64::new(0),
            second_throughput: AtomicU64::new(0),
            second_mean_latency: AtomicU64::new(0),
            second_mean_throughput: AtomicU64::new(0),
            max_throughput: AtomicU64::new(0),
            mean_throughput: AtomicU64::new(0),
            mean_latency: AtomicU64::new(0),
            std_dev_latency: AtomicU64::new(0),
            p50_latency: AtomicU64::new(0),
            p90_latency: AtomicU64::new(0),
            p99_latency: AtomicU64::new(0),
            p999_latency: AtomicU64::new(0),
            event_handler: Arc::new(ClientStatsEvent::new()),
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

    // Do an update w/ a batch event
    pub async fn update_batch(&self, update: ClientStatsUpdate) -> Result<(), FluvioError> {
        self.update(update.clone())?;
        self.event_handler.notify_batch_event().await.unwrap();
        Ok(())
    }

    pub fn get(&self, stat: ClientStatsMetric) -> ClientStatsMetricRaw {
        match stat {
            ClientStatsMetric::StartTime => {
                ClientStatsMetricRaw::StartTime(self.start_time.load(STATS_MEM_ORDER))
            }
            ClientStatsMetric::Uptime => ClientStatsMetricRaw::Uptime(
                unix_timestamp_nanos() - self.start_time.load(STATS_MEM_ORDER),
            ),
            ClientStatsMetric::Pid => ClientStatsMetricRaw::Pid(self.pid.load(STATS_MEM_ORDER)),
            ClientStatsMetric::Offset => {
                ClientStatsMetricRaw::Offset(self.offset.load(STATS_MEM_ORDER))
            }
            ClientStatsMetric::LastBatches => {
                ClientStatsMetricRaw::LastBatches(self.last_batches.load(STATS_MEM_ORDER))
            }
            ClientStatsMetric::LastBytes => {
                ClientStatsMetricRaw::LastBytes(self.last_bytes.load(STATS_MEM_ORDER))
            }
            ClientStatsMetric::LastLatency => {
                ClientStatsMetricRaw::LastLatency(self.last_latency.load(STATS_MEM_ORDER))
            }
            ClientStatsMetric::LastRecords => {
                ClientStatsMetricRaw::LastRecords(self.last_records.load(STATS_MEM_ORDER))
            }
            ClientStatsMetric::LastThroughput => {
                ClientStatsMetricRaw::LastThroughput(self.last_throughput.load(STATS_MEM_ORDER))
            }
            ClientStatsMetric::LastUpdated => {
                ClientStatsMetricRaw::LastUpdated(self.last_updated.load(STATS_MEM_ORDER))
            }
            ClientStatsMetric::Batches => {
                ClientStatsMetricRaw::Batches(self.batches.load(STATS_MEM_ORDER))
            }
            ClientStatsMetric::Bytes => {
                ClientStatsMetricRaw::Bytes(self.bytes.load(STATS_MEM_ORDER))
            }
            ClientStatsMetric::Cpu => ClientStatsMetricRaw::Cpu(self.cpu.load(STATS_MEM_ORDER)),
            ClientStatsMetric::Mem => ClientStatsMetricRaw::Mem(self.mem.load(STATS_MEM_ORDER)),
            ClientStatsMetric::Latency => {
                ClientStatsMetricRaw::Latency(self.latency.load(STATS_MEM_ORDER))
            }
            ClientStatsMetric::Records => {
                ClientStatsMetricRaw::Records(self.records.load(STATS_MEM_ORDER))
            }
            ClientStatsMetric::Throughput => {
                let total_latency = if let ClientStatsMetricRaw::Latency(l) =
                    self.get(ClientStatsMetric::Latency)
                {
                    l
                } else {
                    0
                };
                ClientStatsMetricRaw::Throughput(if total_latency != 0 {
                    self.bytes.load(STATS_MEM_ORDER) / total_latency as u64
                } else {
                    0
                })
            }
            ClientStatsMetric::SecondBatches => {
                ClientStatsMetricRaw::SecondBatches(self.second_batches.load(STATS_MEM_ORDER))
            }
            ClientStatsMetric::SecondLatency => {
                ClientStatsMetricRaw::SecondLatency(self.second_latency.load(STATS_MEM_ORDER))
            }
            ClientStatsMetric::SecondRecords => {
                ClientStatsMetricRaw::SecondRecords(self.second_records.load(STATS_MEM_ORDER))
            }
            ClientStatsMetric::SecondThroughput => {
                ClientStatsMetricRaw::SecondThroughput(self.second_throughput.load(STATS_MEM_ORDER))
            }
            ClientStatsMetric::SecondMeanLatency => ClientStatsMetricRaw::SecondMeanLatency(
                self.second_mean_latency.load(STATS_MEM_ORDER),
            ),
            ClientStatsMetric::SecondMeanThroughput => ClientStatsMetricRaw::SecondMeanThroughput(
                self.second_mean_throughput.load(STATS_MEM_ORDER),
            ),
            ClientStatsMetric::MaxThroughput => {
                ClientStatsMetricRaw::MaxThroughput(self.max_throughput.load(STATS_MEM_ORDER))
            }
            ClientStatsMetric::MeanThroughput => {
                ClientStatsMetricRaw::MeanThroughput(self.mean_throughput.load(STATS_MEM_ORDER))
            }
            ClientStatsMetric::MeanLatency => {
                ClientStatsMetricRaw::MeanLatency(self.mean_latency.load(STATS_MEM_ORDER))
            }
            ClientStatsMetric::StdDevLatency => {
                ClientStatsMetricRaw::StdDevLatency(self.std_dev_latency.load(STATS_MEM_ORDER))
            }
            ClientStatsMetric::P50Latency => {
                ClientStatsMetricRaw::P50Latency(self.p50_latency.load(STATS_MEM_ORDER))
            }
            ClientStatsMetric::P90Latency => {
                ClientStatsMetricRaw::P90Latency(self.p90_latency.load(STATS_MEM_ORDER))
            }
            ClientStatsMetric::P99Latency => {
                ClientStatsMetricRaw::P99Latency(self.p99_latency.load(STATS_MEM_ORDER))
            }
            ClientStatsMetric::P999Latency => {
                ClientStatsMetricRaw::P999Latency(self.p999_latency.load(STATS_MEM_ORDER))
            }
        }
    }

    /// Update the instance with values from `ClientStatsUpdate`
    pub fn update(&self, update: ClientStatsUpdate) -> Result<(), FluvioError> {
        //println!("update: {update:#?}");

        let _: Vec<_> = update
            .data
            .into_iter()
            .map(|data| match data {
                ClientStatsMetricRaw::StartTime(_)
                | ClientStatsMetricRaw::Uptime(_)
                | ClientStatsMetricRaw::Pid(_)
                | ClientStatsMetricRaw::LastBatches(_)
                | ClientStatsMetricRaw::LastBytes(_)
                | ClientStatsMetricRaw::LastLatency(_)
                | ClientStatsMetricRaw::LastRecords(_)
                | ClientStatsMetricRaw::LastThroughput(_)
                | ClientStatsMetricRaw::LastUpdated(_) => Err(FluvioError::Other(format!(
                    "Unsupported stat update: {}",
                    data
                ))),
                ClientStatsMetricRaw::Offset(o) => {
                    self.offset.store(o, STATS_MEM_ORDER);
                    Ok(())
                }
                ClientStatsMetricRaw::Batches(b) => {
                    self.batches.fetch_add(b, STATS_MEM_ORDER);
                    self.last_batches.store(b, STATS_MEM_ORDER);
                    Ok(())
                }
                ClientStatsMetricRaw::Bytes(b) => {
                    self.bytes.fetch_add(b, STATS_MEM_ORDER);
                    self.last_bytes.store(b, STATS_MEM_ORDER);
                    Ok(())
                }
                ClientStatsMetricRaw::Cpu(c) => {
                    self.cpu.store(c, STATS_MEM_ORDER);
                    Ok(())
                }
                ClientStatsMetricRaw::Mem(m) => {
                    self.mem.store(m, STATS_MEM_ORDER);
                    Ok(())
                }
                ClientStatsMetricRaw::Latency(l) => {
                    self.latency.fetch_add(l, STATS_MEM_ORDER);
                    self.last_latency.store(l, STATS_MEM_ORDER);
                    Ok(())
                }
                ClientStatsMetricRaw::Records(r) => {
                    self.records.fetch_add(r, STATS_MEM_ORDER);
                    self.last_records.store(r, STATS_MEM_ORDER);
                    Ok(())
                }
                ClientStatsMetricRaw::Throughput(t) => {
                    self.last_throughput.store(t, STATS_MEM_ORDER);
                    Ok(())
                }
                ClientStatsMetricRaw::SecondBatches(s) => {
                    self.second_batches.store(s, STATS_MEM_ORDER);
                    Ok(())
                }
                ClientStatsMetricRaw::SecondLatency(w) => {
                    self.second_latency.store(w, STATS_MEM_ORDER);
                    Ok(())
                }
                ClientStatsMetricRaw::SecondRecords(w) => {
                    self.second_records.store(w, STATS_MEM_ORDER);
                    Ok(())
                }
                ClientStatsMetricRaw::SecondThroughput(w) => {
                    self.second_throughput.store(w, STATS_MEM_ORDER);
                    Ok(())
                }
                ClientStatsMetricRaw::SecondMeanLatency(w) => {
                    self.second_mean_latency.store(w, STATS_MEM_ORDER);
                    Ok(())
                }
                ClientStatsMetricRaw::SecondMeanThroughput(w) => {
                    self.second_mean_throughput.store(w, STATS_MEM_ORDER);
                    Ok(())
                }
                ClientStatsMetricRaw::MaxThroughput(m) => {
                    self.max_throughput.store(m, STATS_MEM_ORDER);
                    Ok(())
                }
                ClientStatsMetricRaw::MeanThroughput(a) => {
                    self.mean_throughput.store(a, STATS_MEM_ORDER);
                    Ok(())
                }
                ClientStatsMetricRaw::MeanLatency(a) => {
                    self.mean_latency.store(a, STATS_MEM_ORDER);
                    Ok(())
                }
                ClientStatsMetricRaw::StdDevLatency(s) => {
                    self.std_dev_latency.store(s, STATS_MEM_ORDER);
                    Ok(())
                }
                ClientStatsMetricRaw::P50Latency(p) => {
                    self.p50_latency.store(p, STATS_MEM_ORDER);
                    Ok(())
                }
                ClientStatsMetricRaw::P90Latency(p) => {
                    self.p90_latency.store(p, STATS_MEM_ORDER);
                    Ok(())
                }
                ClientStatsMetricRaw::P99Latency(p) => {
                    self.p99_latency.store(p, STATS_MEM_ORDER);
                    Ok(())
                }
                ClientStatsMetricRaw::P999Latency(p) => {
                    self.p999_latency.store(p, STATS_MEM_ORDER);
                    Ok(())
                }
            })
            .collect();

        self.last_updated
            .store(unix_timestamp_nanos(), STATS_MEM_ORDER);

        //println!("{:#?}", &self);
        Ok(())
    }

    /// Return the current `ClientStats` as `ClientStatsDataPoint`
    pub fn get_datapoint(&self) -> ClientStatsDataPoint {
        self.into()
    }

    /// Returns the current uptime of the client in nanoseconds
    pub fn uptime(&self) -> i64 {
        unix_timestamp_nanos() - self.start_time.load(STATS_MEM_ORDER)
    }

    /// Record the latency of a producer request
    pub async fn send_and_measure_latency(
        &self,
        socket: &VersionedSerialSocket,
        request: ProduceRequest<RecordSet<RawRecords>>,
        batch_bytes: u64,
    ) -> Result<(ProduceResponse, ClientStatsUpdate)> {
        let send_start_time = Instant::now();

        let response = socket.send_receive(request).await?;

        let send_latency = send_start_time.elapsed().as_nanos() as u64;

        let mut stats_update = ClientStatsUpdate::new();
        stats_update.push(ClientStatsMetricRaw::Latency(send_latency));

        if batch_bytes > 0 {
            // Bytes per nanosecond
            stats_update.push(ClientStatsMetricRaw::Throughput(
                (batch_bytes / send_latency) as u64,
            ));
        }

        Ok((response, stats_update))
    }
}

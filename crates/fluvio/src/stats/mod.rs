mod update_builder;
mod monitor;
pub use monitor::ClientStatsEvent;
pub mod metrics;
pub use metrics::{
    ClientStatsMetric, ClientStatsMetricRaw, ClientStatsMetricFormat, ClientStatsDataFrame,
    ClientStatsHistogram,
};
mod config;
pub use config::ClientStatsDataCollect;
use quantities::LinearScaledUnit;
use quantities::duration::NANOSECOND;
pub use update_builder::ClientStatsUpdateBuilder;

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
    /// Throughput the last transfer, in bytes per second
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
pub(crate) fn unix_timestamp_nanos() -> i64 {
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

    /// Return configured collection option
    pub fn stats_collect(&self) -> ClientStatsDataCollect {
        self.stats_collect
    }

    // Do an update w/ a batch event
    pub async fn update_batch(&self, update: ClientStatsUpdateBuilder) -> Result<(), FluvioError> {
        // Find the bytes and latency (convert to seconds)

        let latency = update.data.iter().find_map(|d| {
            if let ClientStatsMetricRaw::Latency(l) = d {
                // Convert Nanoseconds to seconds
                #[cfg(not(target_arch = "wasm32"))]
                let seconds = *l as f64 * NANOSECOND.scale();
                #[cfg(target_arch = "wasm32")]
                let seconds = *l as f32 * NANOSECOND.scale();

                Some(seconds)
            } else {
                None
            }
        });

        let bytes = update.data.iter().find_map(|d| {
            if let ClientStatsMetricRaw::Bytes(b) = d {
                #[cfg(not(target_arch = "wasm32"))]
                let report_bytes = Some(*b as f64);
                #[cfg(target_arch = "wasm32")]
                let report_bytes = Some(*b as f32);
                report_bytes
            } else {
                None
            }
        });

        // then calculate the throughput
        let mut batch_stats = update.clone();
        if let (Some(l), Some(b)) = (latency, bytes) {
            batch_stats.push(ClientStatsMetricRaw::Throughput((b / l) as u64));
        }

        self.update(batch_stats)?;
        self.event_handler.notify_batch_event().await.unwrap();
        Ok(())
    }

    pub fn get(&self, stat: ClientStatsMetric) -> ClientStatsMetricRaw {
        match stat {
            ClientStatsMetric::StartTime => {
                ClientStatsMetricRaw::StartTime(self.start_time.load(STATS_MEM_ORDER))
            }
            ClientStatsMetric::RunTime => ClientStatsMetricRaw::RunTime(
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
                let run_time = if let ClientStatsMetricRaw::RunTime(u) =
                    self.get(ClientStatsMetric::RunTime)
                {
                    // Convert Nanoseconds to seconds
                    #[cfg(not(target_arch = "wasm32"))]
                    let seconds = u as f64 * NANOSECOND.scale();
                    #[cfg(target_arch = "wasm32")]
                    let seconds = u as f32 * NANOSECOND.scale();

                    //println!("seconds {seconds}");
                    Some(seconds)
                } else {
                    None
                };

                #[cfg(not(target_arch = "wasm32"))]
                let total_bytes = self.bytes.load(STATS_MEM_ORDER) as f64;
                #[cfg(target_arch = "wasm32")]
                let total_bytes = self.bytes.load(STATS_MEM_ORDER) as f32;

                //println!("total_bytes {total_bytes}");
                //println!("Throughput {}", total_bytes / run_time.clone().unwrap());

                ClientStatsMetricRaw::Throughput(if let Some(up) = run_time {
                    if up != 0.0 {
                        (total_bytes / up) as u64
                    } else {
                        0
                    }
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

    /// Update the instance with values from `ClientStatsUpdateBuilder`
    pub fn update(&self, update: ClientStatsUpdateBuilder) -> Result<(), FluvioError> {
        //println!("update: {update:#?}");

        let _: Vec<_> = update
            .data
            .into_iter()
            .map(|data| match data {
                ClientStatsMetricRaw::StartTime(_)
                | ClientStatsMetricRaw::RunTime(_)
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

        //println!("After update: {:#?}", &self);
        Ok(())
    }

    /// Return the current `ClientStats` as `ClientStatsDataFrame`
    pub fn get_dataframe(&self) -> ClientStatsDataFrame {
        self.into()
    }

    /// Returns the current running time of the client in nanoseconds
    pub fn run_time(&self) -> i64 {
        unix_timestamp_nanos() - self.start_time.load(STATS_MEM_ORDER)
    }

    /// Record the latency of a producer request
    pub async fn send_and_measure_latency(
        &self,
        socket: &VersionedSerialSocket,
        request: ProduceRequest<RecordSet<RawRecords>>,
        batch_bytes: u64,
    ) -> Result<(ProduceResponse, ClientStatsUpdateBuilder)> {
        let send_start_time = Instant::now();

        let response = socket.send_receive(request).await?;

        let send_latency = send_start_time.elapsed().as_nanos() as u64;

        let mut stats_update = ClientStatsUpdateBuilder::new();
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

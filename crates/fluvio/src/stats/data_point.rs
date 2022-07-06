use crate::FluvioError;
use super::{ClientStats, ClientStatsDataCollect, ClientStatsMetricRaw};
use serde::{Serialize, Deserialize};

use strum::{EnumIter, IntoEnumIterator};
use std::string::ToString;
use quantities::{prelude::*, AMNT_ONE, AMNT_ZERO};
use quantities::datathroughput::{
    DataThroughput, TERABYTE_PER_SECOND, GIGABYTE_PER_SECOND, MEGABYTE_PER_SECOND,
    KILOBYTE_PER_SECOND, BYTE_PER_SECOND,
};
use quantities::datavolume::{DataVolume, BYTE, KILOBYTE, MEGABYTE, GIGABYTE, TERABYTE};
use quantities::duration::{
    Duration as QuantDuration, MINUTE, SECOND, MILLISECOND, MICROSECOND, NANOSECOND,
};

#[derive(Debug, Clone, Copy, EnumIter)]
pub enum ClientStatsMetric {
    #[strum(serialize = "start_time")]
    StartTime,
    #[strum(serialize = "up_time")]
    Uptime,
    #[strum(serialize = "pid")]
    Pid,
    #[strum(serialize = "offset")]
    Offset,
    #[strum(serialize = "last_batches")]
    LastBatches,
    #[strum(serialize = "last_bytes")]
    LastBytes,
    #[strum(serialize = "last_latency")]
    LastLatency,
    #[strum(serialize = "last_records")]
    LastRecords,
    #[strum(serialize = "last_throughput")]
    LastThroughput,
    #[strum(serialize = "last_updated")]
    LastUpdated,
    #[strum(serialize = "batches")]
    Batches,
    #[strum(serialize = "bytes")]
    Bytes,
    #[strum(serialize = "cpu")]
    Cpu,
    #[strum(serialize = "mem")]
    Mem,
    #[strum(serialize = "latency")]
    Latency,
    #[strum(serialize = "records")]
    Records,
    #[strum(serialize = "throughput")]
    Throughput,
    #[strum(serialize = "second_batches")]
    SecondBatches,
    #[strum(serialize = "second_latency")]
    SecondLatency,
    #[strum(serialize = "second_records")]
    SecondRecords,
    #[strum(serialize = "second_throughput")]
    SecondThroughput,
    #[strum(serialize = "second_mean_latency")]
    SecondMeanLatency,
    #[strum(serialize = "second_mean_throughput")]
    SecondMeanThroughput,
    #[strum(serialize = "max_throughput")]
    MaxThroughput,
    #[strum(serialize = "mean_throughput")]
    MeanThroughput,
    #[strum(serialize = "mean_latency")]
    MeanLatency,
    #[strum(serialize = "std_dev_latency")]
    StdDevLatency,
    #[strum(serialize = "p50_latency")]
    P50Latency,
    #[strum(serialize = "p90_latency")]
    P90Latency,
    #[strum(serialize = "p99_latency")]
    P99Latency,
    #[strum(serialize = "p999_latency")]
    P999Latency,
}

#[derive(Debug, Clone, Copy)]
pub enum ClientStatsMetricFormat {
    StartTime(i64),
    Uptime(QuantDuration),
    Pid(u32),
    Offset(i32),
    LastBatches(u64),
    LastBytes(DataVolume),
    LastLatency(QuantDuration),
    LastRecords(u64),
    LastThroughput(DataThroughput),
    LastUpdated(i64),
    Batches(u64),
    Bytes(DataVolume),
    Cpu(f32),
    Mem(DataVolume),
    Latency(QuantDuration),
    Records(u64),
    Throughput(DataThroughput),
    SecondBatches(u64),
    SecondLatency(QuantDuration),
    SecondRecords(u64),
    SecondThroughput(DataVolume),
    SecondMeanLatency(QuantDuration),
    SecondMeanThroughput(DataThroughput),
    MaxThroughput(DataThroughput),
    MeanThroughput(DataThroughput),
    MeanLatency(QuantDuration),
    StdDevLatency(QuantDuration),
    P50Latency(QuantDuration),
    P90Latency(QuantDuration),
    P99Latency(QuantDuration),
    P999Latency(QuantDuration),
}

impl ClientStatsMetricFormat {
    pub fn value_to_string(&self) -> String {
        match self {
            Self::StartTime(n) => n.to_string(),
            Self::Uptime(n) => format!("{:<15.3}", n),
            Self::Pid(n) => n.to_string(),
            Self::Offset(n) => n.to_string(),
            Self::LastBatches(n) => n.to_string(),
            Self::LastBytes(n) => format!("{:<15.3}", n),
            Self::LastLatency(n) => format!("{:<15.3}", n),
            Self::LastRecords(n) => n.to_string(),
            Self::LastThroughput(n) => format!("{:<15.3}", n),
            Self::LastUpdated(n) => n.to_string(),
            Self::Batches(n) => n.to_string(),
            Self::Bytes(n) => format!("{:<15.3}", n),
            Self::Cpu(n) => n.to_string(),
            Self::Mem(n) => format!("{:<15.3}", n),
            Self::Latency(n) => format!("{:<15.3}", n),
            Self::Records(n) => n.to_string(),
            Self::Throughput(n) => format!("{:<15.3}", n),
            Self::SecondBatches(n) => n.to_string(),
            Self::SecondLatency(n) => format!("{:<15.3}", n),
            Self::SecondRecords(n) => n.to_string(),
            Self::SecondThroughput(n) => format!("{:<15.3}", n),
            Self::SecondMeanLatency(n) => format!("{:<15.3}", n),
            Self::SecondMeanThroughput(n) => format!("{:<15.3}", n),
            Self::MaxThroughput(n) => format!("{:<15.3}", n),
            Self::MeanThroughput(n) => format!("{:<15.3}", n),
            Self::MeanLatency(n) => format!("{:<15.3}", n),
            Self::StdDevLatency(n) => format!("{:<15.3}", n),
            Self::P50Latency(n) => format!("{:<15.3}", n),
            Self::P90Latency(n) => format!("{:<15.3}", n),
            Self::P99Latency(n) => format!("{:<15.3}", n),
            Self::P999Latency(n) => format!("{:<15.3}", n),
        }
    }
}

// TODO: Convert `throughput` and `last_throughput` to bytes per second
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct ClientStatsDataPoint {
    /// Start time when struct was created
    /// This is Unix Epoch time, in nanoseconds
    start_time: i64,
    /// This is the elapsed time in nanoseconds
    uptime: i64,
    /// PID of the client process
    pid: u32,
    /// Offset from last batch seen
    offset: i32,
    /// The number of batches process in last transfer
    last_batches: u64,
    /// Data volume of last data transfer, in bytes
    last_bytes: u64,
    /// Time it took to complete last data transfer, in nanoseconds
    last_latency: u64,
    /// The number of records in the last transfer
    last_records: u64,
    /// TODO: Convert to bytes per second. The throughput of the last transfer, in bytes per nanosecond
    last_throughput: u64,
    /// Last time any struct values were updated
    /// This is Unix Epoch time, in nanoseconds
    last_updated: i64,
    /// Total number of batches processed
    batches: u64,
    /// Data volume of all data transferred, in bytes
    bytes: u64,
    /// Last polled CPU usage in percent, adjusted for host system's # of CPU cores
    cpu: f32,
    /// Last polled memory usage of client process, in kilobytes
    mem: u64,
    /// Total number of records processed
    records: u64,
    /// The throughput of current session, in bytes per nanosecond
    throughput: u64,
    /// Batches per second
    second_batches: u64,
    /// Latency in nanoseconds per second
    second_latency: u64,
    /// Records per second
    second_records: u64,
    /// Throughput in bytes per second
    second_throughput: u64,
    /// Mean Latency per second
    second_mean_latency: u64,
    /// Mean Throughput per second
    second_mean_throughput: u64,
    /// Maximum throughput recorded
    max_throughput: u64,
    /// Mean Throughput
    mean_throughput: u64,
    /// Mean latency
    mean_latency: u64,
    /// Standard deviation latency
    std_dev_latency: u64,
    /// P50 latency
    p50_latency: u64,
    /// P90 latency
    p90_latency: u64,
    /// P99 latency
    p99_latency: u64,
    /// P999 latency
    p999_latency: u64,
    /// Configuration of what data client is to collect
    stats_collect: ClientStatsDataCollect,
}

/// This copies from `ClientStats` passes values through to its respective field.
/// The exceptions to value pass-through are:
/// * CPU - which is converted to human-readable percentage
/// * Throughput and Last Throughput - which is converted from bytes per nanosecond to bytes per second
impl From<&ClientStats> for ClientStatsDataPoint {
    fn from(current: &ClientStats) -> Self {
        let mut data_point = Self::default();

        for metric in ClientStatsMetric::iter() {
            match metric {
                ClientStatsMetric::StartTime => {
                    let start_time = if let ClientStatsMetricRaw::StartTime(start_time) =
                        current.get(ClientStatsMetric::StartTime)
                    {
                        start_time
                    } else {
                        0
                    };

                    data_point.start_time = start_time;
                }
                ClientStatsMetric::Uptime => {
                    let uptime = if let ClientStatsMetricRaw::Uptime(uptime) =
                        current.get(ClientStatsMetric::Uptime)
                    {
                        uptime
                    } else {
                        0
                    };

                    data_point.uptime = uptime;
                }
                ClientStatsMetric::Pid => {
                    let pid = if let ClientStatsMetricRaw::Pid(pid) =
                        current.get(ClientStatsMetric::Pid)
                    {
                        pid
                    } else {
                        0
                    };

                    data_point.pid = pid;
                }
                ClientStatsMetric::Offset => {
                    let offset = if let ClientStatsMetricRaw::Offset(offset) =
                        current.get(ClientStatsMetric::Offset)
                    {
                        offset
                    } else {
                        0
                    };

                    data_point.offset = offset;
                }
                ClientStatsMetric::LastBatches => {
                    let last_batches = if let ClientStatsMetricRaw::LastBatches(last_batches) =
                        current.get(ClientStatsMetric::LastBatches)
                    {
                        last_batches
                    } else {
                        0
                    };

                    data_point.last_batches = last_batches;
                }
                ClientStatsMetric::LastBytes => {
                    let last_bytes = if let ClientStatsMetricRaw::LastBytes(last_bytes) =
                        current.get(ClientStatsMetric::LastBytes)
                    {
                        last_bytes
                    } else {
                        0
                    };
                    data_point.last_bytes = last_bytes;
                }
                ClientStatsMetric::LastLatency => {
                    let last_latency = if let ClientStatsMetricRaw::LastLatency(last_latency) =
                        current.get(ClientStatsMetric::LastLatency)
                    {
                        last_latency
                    } else {
                        0
                    };
                    data_point.last_latency = last_latency;
                }
                ClientStatsMetric::LastRecords => {
                    let last_records = if let ClientStatsMetricRaw::LastRecords(last_records) =
                        current.get(ClientStatsMetric::LastRecords)
                    {
                        last_records
                    } else {
                        0
                    };
                    data_point.last_records = last_records;
                }
                ClientStatsMetric::LastThroughput => {
                    let last_throughput =
                        if let ClientStatsMetricRaw::LastThroughput(last_throughput) =
                            current.get(ClientStatsMetric::LastThroughput)
                        {
                            last_throughput
                        } else {
                            0
                        };

                    #[cfg(not(target_arch = "wasm32"))]
                    let bytes_per_second = (last_throughput as f64 * NANOSECOND.scale()) as u64;
                    #[cfg(target_arch = "wasm32")]
                    let bytes_per_second = (last_throughput as f32 * NANOSECOND.scale()) as u64;

                    data_point.last_throughput = bytes_per_second;
                }
                ClientStatsMetric::LastUpdated => {
                    let last_updated = if let ClientStatsMetricRaw::LastUpdated(last_updated) =
                        current.get(ClientStatsMetric::LastUpdated)
                    {
                        last_updated
                    } else {
                        0
                    };
                    data_point.last_updated = last_updated;
                }
                ClientStatsMetric::Batches => {
                    let batches = if let ClientStatsMetricRaw::Batches(batches) =
                        current.get(ClientStatsMetric::Batches)
                    {
                        batches
                    } else {
                        0
                    };
                    data_point.batches = batches;
                }
                ClientStatsMetric::Bytes => {
                    let bytes = if let ClientStatsMetricRaw::Bytes(bytes) =
                        current.get(ClientStatsMetric::Bytes)
                    {
                        bytes
                    } else {
                        0
                    };
                    data_point.bytes = bytes;
                }
                ClientStatsMetric::Cpu => {
                    let cpu = if let ClientStatsMetricRaw::Cpu(cpu) =
                        current.get(ClientStatsMetric::Cpu)
                    {
                        cpu
                    } else {
                        0
                    };
                    // Converting to human-readable percent
                    data_point.cpu = Self::format_cpu_raw_to_percent(cpu);
                }
                ClientStatsMetric::Mem => {
                    let mem = if let ClientStatsMetricRaw::Mem(mem) =
                        current.get(ClientStatsMetric::Mem)
                    {
                        mem
                    } else {
                        0
                    };
                    data_point.mem = mem;
                }
                ClientStatsMetric::Latency => {
                    let last_latency = if let ClientStatsMetricRaw::Latency(last_latency) =
                        current.get(ClientStatsMetric::Latency)
                    {
                        last_latency
                    } else {
                        0
                    };
                    data_point.last_latency = last_latency;
                }
                ClientStatsMetric::Records => {
                    let records = if let ClientStatsMetricRaw::Records(records) =
                        current.get(ClientStatsMetric::Records)
                    {
                        records
                    } else {
                        0
                    };
                    data_point.records = records;
                }
                ClientStatsMetric::Throughput => {
                    let throughput = if let ClientStatsMetricRaw::Throughput(throughput) =
                        current.get(ClientStatsMetric::Throughput)
                    {
                        throughput
                    } else {
                        0
                    };

                    #[cfg(not(target_arch = "wasm32"))]
                    let bytes_per_second = (throughput as f64 * NANOSECOND.scale()) as u64;
                    #[cfg(target_arch = "wasm32")]
                    let bytes_per_second = (throughput as f32 * NANOSECOND.scale()) as u64;

                    data_point.last_throughput = bytes_per_second;
                }
                ClientStatsMetric::SecondBatches => {
                    let second_batches =
                        if let ClientStatsMetricRaw::SecondBatches(second_batches) =
                            current.get(ClientStatsMetric::SecondBatches)
                        {
                            second_batches
                        } else {
                            0
                        };
                    data_point.second_batches = second_batches;
                }
                ClientStatsMetric::SecondLatency => {
                    let second_latency =
                        if let ClientStatsMetricRaw::SecondLatency(second_latency) =
                            current.get(ClientStatsMetric::SecondLatency)
                        {
                            second_latency
                        } else {
                            0
                        };
                    data_point.second_latency = second_latency;
                }
                ClientStatsMetric::SecondRecords => {
                    let second_records =
                        if let ClientStatsMetricRaw::SecondRecords(second_records) =
                            current.get(ClientStatsMetric::SecondRecords)
                        {
                            second_records
                        } else {
                            0
                        };
                    data_point.second_records = second_records;
                }
                ClientStatsMetric::SecondThroughput => {
                    let second_throughput =
                        if let ClientStatsMetricRaw::SecondThroughput(second_throughput) =
                            current.get(ClientStatsMetric::SecondThroughput)
                        {
                            second_throughput
                        } else {
                            0
                        };
                    data_point.second_throughput = second_throughput;
                }
                ClientStatsMetric::SecondMeanLatency => {
                    let second_mean_latency =
                        if let ClientStatsMetricRaw::SecondMeanLatency(second_mean_latency) =
                            current.get(ClientStatsMetric::SecondMeanLatency)
                        {
                            second_mean_latency
                        } else {
                            0
                        };
                    data_point.second_mean_latency = second_mean_latency;
                }
                ClientStatsMetric::SecondMeanThroughput => {
                    let second_mean_throughput =
                        if let ClientStatsMetricRaw::SecondMeanThroughput(second_mean_throughput) =
                            current.get(ClientStatsMetric::SecondMeanThroughput)
                        {
                            second_mean_throughput
                        } else {
                            0
                        };
                    data_point.second_mean_throughput = second_mean_throughput;
                }
                ClientStatsMetric::MaxThroughput => {
                    let max_throughput =
                        if let ClientStatsMetricRaw::MaxThroughput(max_throughput) =
                            current.get(ClientStatsMetric::MaxThroughput)
                        {
                            max_throughput
                        } else {
                            0
                        };
                    data_point.max_throughput = max_throughput;
                }
                ClientStatsMetric::MeanThroughput => {
                    let mean_throughput =
                        if let ClientStatsMetricRaw::MeanThroughput(mean_throughput) =
                            current.get(ClientStatsMetric::MeanThroughput)
                        {
                            mean_throughput
                        } else {
                            0
                        };

                    #[cfg(not(target_arch = "wasm32"))]
                    let bytes_per_second = (mean_throughput as f64 * NANOSECOND.scale()) as u64;
                    #[cfg(target_arch = "wasm32")]
                    let bytes_per_second = (mean_throughput as f32 * NANOSECOND.scale()) as u64;

                    data_point.last_throughput = bytes_per_second;
                }
                ClientStatsMetric::MeanLatency => {
                    let mean_latency = if let ClientStatsMetricRaw::MeanLatency(mean_latency) =
                        current.get(ClientStatsMetric::MeanLatency)
                    {
                        mean_latency
                    } else {
                        0
                    };
                    data_point.mean_latency = mean_latency;
                }
                ClientStatsMetric::StdDevLatency => {
                    let std_dev_latency =
                        if let ClientStatsMetricRaw::StdDevLatency(std_dev_latency) =
                            current.get(ClientStatsMetric::StdDevLatency)
                        {
                            std_dev_latency
                        } else {
                            0
                        };
                    data_point.std_dev_latency = std_dev_latency;
                }
                ClientStatsMetric::P50Latency => {
                    let p50 = if let ClientStatsMetricRaw::P50Latency(p50) =
                        current.get(ClientStatsMetric::P50Latency)
                    {
                        p50
                    } else {
                        0
                    };
                    data_point.p50_latency = p50;
                }
                ClientStatsMetric::P90Latency => {
                    let p90 = if let ClientStatsMetricRaw::P90Latency(p90) =
                        current.get(ClientStatsMetric::P90Latency)
                    {
                        p90
                    } else {
                        0
                    };
                    data_point.p90_latency = p90;
                }
                ClientStatsMetric::P99Latency => {
                    let p99 = if let ClientStatsMetricRaw::P99Latency(p99) =
                        current.get(ClientStatsMetric::P99Latency)
                    {
                        p99
                    } else {
                        0
                    };
                    data_point.p99_latency = p99;
                }
                ClientStatsMetric::P999Latency => {
                    let p999 = if let ClientStatsMetricRaw::P999Latency(p999) =
                        current.get(ClientStatsMetric::P999Latency)
                    {
                        p999
                    } else {
                        0
                    };
                    data_point.p999_latency = p999;
                }
            };
        }

        data_point.stats_collect = current.stats_collect;

        data_point
    }
}

impl ClientStatsDataPoint {
    pub fn get(&self, stat: ClientStatsMetric) -> ClientStatsMetricRaw {
        match stat {
            ClientStatsMetric::StartTime => ClientStatsMetricRaw::StartTime(self.start_time),
            ClientStatsMetric::Uptime => ClientStatsMetricRaw::Uptime(self.start_time),
            ClientStatsMetric::Pid => ClientStatsMetricRaw::Pid(self.pid),
            ClientStatsMetric::Offset => ClientStatsMetricRaw::Offset(self.offset),
            ClientStatsMetric::LastBatches => ClientStatsMetricRaw::LastBatches(self.last_batches),
            ClientStatsMetric::LastBytes => ClientStatsMetricRaw::LastBytes(self.last_bytes),
            ClientStatsMetric::LastLatency => ClientStatsMetricRaw::LastLatency(self.last_latency),
            ClientStatsMetric::LastRecords => ClientStatsMetricRaw::LastRecords(self.last_records),
            ClientStatsMetric::LastThroughput => {
                ClientStatsMetricRaw::LastThroughput(self.last_throughput)
            }
            ClientStatsMetric::LastUpdated => ClientStatsMetricRaw::LastUpdated(self.last_updated),
            ClientStatsMetric::Batches => ClientStatsMetricRaw::Batches(self.batches),
            ClientStatsMetric::Bytes => ClientStatsMetricRaw::Bytes(self.bytes),
            ClientStatsMetric::Cpu => {
                ClientStatsMetricRaw::Cpu(Self::percent_cpu_to_cpu_raw(self.cpu))
            }
            ClientStatsMetric::Mem => ClientStatsMetricRaw::Mem(self.mem),
            ClientStatsMetric::Latency => ClientStatsMetricRaw::Latency(self.last_latency),
            ClientStatsMetric::Records => ClientStatsMetricRaw::Records(self.records),
            ClientStatsMetric::Throughput => ClientStatsMetricRaw::Throughput(self.throughput),
            ClientStatsMetric::SecondBatches => {
                ClientStatsMetricRaw::SecondBatches(self.second_batches)
            }
            ClientStatsMetric::SecondLatency => {
                ClientStatsMetricRaw::SecondLatency(self.second_latency)
            }
            ClientStatsMetric::SecondRecords => {
                ClientStatsMetricRaw::SecondRecords(self.second_records)
            }
            ClientStatsMetric::SecondThroughput => {
                ClientStatsMetricRaw::SecondThroughput(self.second_throughput)
            }
            ClientStatsMetric::SecondMeanLatency => {
                ClientStatsMetricRaw::SecondMeanLatency(self.second_mean_latency)
            }
            ClientStatsMetric::SecondMeanThroughput => {
                ClientStatsMetricRaw::SecondMeanThroughput(self.second_mean_throughput)
            }
            ClientStatsMetric::MaxThroughput => {
                ClientStatsMetricRaw::MaxThroughput(self.max_throughput)
            }
            ClientStatsMetric::MeanThroughput => {
                ClientStatsMetricRaw::MeanThroughput(self.mean_throughput)
            }
            ClientStatsMetric::MeanLatency => ClientStatsMetricRaw::MeanLatency(self.mean_latency),
            ClientStatsMetric::StdDevLatency => {
                ClientStatsMetricRaw::StdDevLatency(self.std_dev_latency)
            }
            ClientStatsMetric::P50Latency => ClientStatsMetricRaw::P50Latency(self.p50_latency),
            ClientStatsMetric::P90Latency => ClientStatsMetricRaw::P90Latency(self.p90_latency),
            ClientStatsMetric::P99Latency => ClientStatsMetricRaw::P99Latency(self.p99_latency),
            ClientStatsMetric::P999Latency => ClientStatsMetricRaw::P999Latency(self.p999_latency),
        }
    }

    pub fn get_format(&self, stat: ClientStatsMetric) -> ClientStatsMetricFormat {
        match stat {
            ClientStatsMetric::StartTime => ClientStatsMetricFormat::StartTime(self.start_time),
            ClientStatsMetric::Uptime => ClientStatsMetricFormat::Uptime(
                Self::format_duration_from_nanos(self.uptime as u64),
            ),
            ClientStatsMetric::Pid => ClientStatsMetricFormat::Pid(self.pid),
            ClientStatsMetric::Offset => ClientStatsMetricFormat::Offset(self.offset),
            ClientStatsMetric::LastBatches => {
                ClientStatsMetricFormat::LastBatches(self.last_batches)
            }
            ClientStatsMetric::LastBytes => {
                ClientStatsMetricFormat::LastBytes(Self::format_data_volume(self.last_bytes))
            }
            ClientStatsMetric::LastLatency => ClientStatsMetricFormat::LastLatency(
                Self::format_duration_from_nanos(self.last_latency),
            ),
            ClientStatsMetric::LastRecords => {
                ClientStatsMetricFormat::LastRecords(self.last_records)
            }
            ClientStatsMetric::LastThroughput => {
                let bytes = if let ClientStatsMetricFormat::LastBytes(bytes) =
                    self.get_format(ClientStatsMetric::LastBytes)
                {
                    bytes
                } else {
                    AMNT_ZERO * BYTE
                };
                let latency = if let ClientStatsMetricFormat::LastLatency(latency) =
                    self.get_format(ClientStatsMetric::LastLatency)
                {
                    latency
                } else {
                    AMNT_ZERO * SECOND
                };

                ClientStatsMetricFormat::LastThroughput(bytes / latency)
            }
            ClientStatsMetric::LastUpdated => {
                ClientStatsMetricFormat::LastUpdated(self.last_updated)
            }
            ClientStatsMetric::Batches => ClientStatsMetricFormat::Batches(self.batches),
            ClientStatsMetric::Bytes => {
                ClientStatsMetricFormat::Bytes(Self::format_data_volume(self.bytes))
            }
            ClientStatsMetric::Cpu => ClientStatsMetricFormat::Cpu(self.cpu),
            ClientStatsMetric::Mem => ClientStatsMetricFormat::Mem(Self::format_memory(self.mem)),
            ClientStatsMetric::Latency => ClientStatsMetricFormat::Latency(
                Self::format_duration_from_nanos(self.last_latency),
            ),
            ClientStatsMetric::Records => ClientStatsMetricFormat::Records(self.records),
            ClientStatsMetric::Throughput => {
                let bytes = if let ClientStatsMetricFormat::Bytes(bytes) =
                    self.get_format(ClientStatsMetric::Bytes)
                {
                    bytes
                } else {
                    AMNT_ZERO * BYTE
                };
                let latency = if let ClientStatsMetricFormat::Latency(latency) =
                    self.get_format(ClientStatsMetric::Latency)
                {
                    latency
                } else {
                    AMNT_ZERO * SECOND
                };

                ClientStatsMetricFormat::Throughput(bytes / latency)
            }
            ClientStatsMetric::SecondBatches => {
                ClientStatsMetricFormat::SecondBatches(self.second_batches)
            }
            ClientStatsMetric::SecondLatency => ClientStatsMetricFormat::SecondLatency(
                Self::format_duration_from_nanos(self.second_latency),
            ),
            ClientStatsMetric::SecondRecords => {
                ClientStatsMetricFormat::SecondRecords(self.second_records)
            }
            ClientStatsMetric::SecondThroughput => ClientStatsMetricFormat::SecondThroughput(
                Self::format_data_volume(self.second_throughput),
            ),
            ClientStatsMetric::SecondMeanLatency => ClientStatsMetricFormat::SecondMeanLatency(
                Self::format_duration_from_nanos(self.second_mean_latency),
            ),
            ClientStatsMetric::SecondMeanThroughput => {
                ClientStatsMetricFormat::SecondMeanThroughput(Self::format_throughput(
                    self.second_mean_throughput,
                ))
            }
            ClientStatsMetric::MaxThroughput => {
                ClientStatsMetricFormat::MaxThroughput(Self::format_throughput(self.max_throughput))
            }
            ClientStatsMetric::MeanThroughput => ClientStatsMetricFormat::MeanThroughput(
                Self::format_throughput(self.mean_throughput),
            ),
            ClientStatsMetric::MeanLatency => ClientStatsMetricFormat::MeanLatency(
                Self::format_duration_from_nanos(self.mean_latency),
            ),
            ClientStatsMetric::StdDevLatency => ClientStatsMetricFormat::StdDevLatency(
                Self::format_duration_from_nanos(self.std_dev_latency),
            ),
            ClientStatsMetric::P50Latency => ClientStatsMetricFormat::P50Latency(
                Self::format_duration_from_nanos(self.p50_latency),
            ),
            ClientStatsMetric::P90Latency => ClientStatsMetricFormat::P90Latency(
                Self::format_duration_from_nanos(self.p90_latency),
            ),
            ClientStatsMetric::P99Latency => ClientStatsMetricFormat::P99Latency(
                Self::format_duration_from_nanos(self.p99_latency),
            ),
            ClientStatsMetric::P999Latency => ClientStatsMetricFormat::P999Latency(
                Self::format_duration_from_nanos(self.p999_latency),
            ),
        }
    }

    /// Return configured collection option
    pub fn stats_collect(&self) -> ClientStatsDataCollect {
        self.stats_collect
    }

    // This should not need ClientStatsMetricRaw. Just *Metric
    // Can I get strum to help me with this?
    /// Generates a CSV header string. Columns returned based on client's configured `ClientStatsDataCollect`
    pub fn csv_header(&self, stats: &Vec<ClientStatsMetric>) -> Result<String, FluvioError> {
        let h_list: Vec<String> = stats
            .into_iter()
            .map(|s| {
                let d = self.get(*s);
                d.to_string()
            })
            .collect();

        // Add commas and append newline
        let header = vec![h_list.join(","), "\n".to_string()].join("");

        Ok(header)
    }

    // This should not need ClientStatsMetricRaw. Just *Metric
    /// Generates a CSV row. Columns returned based on client's configured `ClientStatsDataCollect`
    pub fn csv_datapoint(&self, stats: &Vec<ClientStatsMetric>) -> Result<String, FluvioError> {
        let d: Vec<String> = stats
            .into_iter()
            .map(|s| {
                let d = self.get(*s);

                d.value_to_string()
            })
            .collect();
        let csv_datapoint = vec![d.join(","), "\n".to_string()].join("");
        Ok(csv_datapoint)
    }

    /// Convert recorded cpu back to human readable percentage
    fn format_cpu_raw_to_percent(cpu_raw: u32) -> f32 {
        cpu_raw as f32 / 1_000.0
    }

    /// Convert cpu as human readable percentage back to raw recorded form
    fn percent_cpu_to_cpu_raw(cpu_raw: f32) -> u32 {
        cpu_raw as u32 * 1_000
    }

    /// Format time units for Display
    fn format_duration_from_nanos(nanoseconds: u64) -> QuantDuration {
        #[cfg(not(target_arch = "wasm32"))]
        let scalar: AmountT = (nanoseconds as u32).into();
        #[cfg(target_arch = "wasm32")]
        let scalar: AmountT = (nanoseconds as u16).into();
        ClientStatsDataPoint::convert_to_largest_time_unit(scalar * NANOSECOND)
    }

    /// Convert given time quantity into largest divisible unit type of `second`
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

    /// Format data volume units for Display
    fn format_data_volume(data: u64) -> DataVolume {
        #[cfg(not(target_arch = "wasm32"))]
        let scalar: AmountT = (data as u32).into();
        #[cfg(target_arch = "wasm32")]
        let scalar: AmountT = (data as u16).into();
        ClientStatsDataPoint::convert_to_largest_data_unit(scalar * BYTE)
    }

    /// Format memory units for Display
    fn format_memory(mem: u64) -> DataVolume {
        #[cfg(not(target_arch = "wasm32"))]
        let scalar: AmountT = (mem as u32).into();
        #[cfg(target_arch = "wasm32")]
        let scalar: AmountT = (mem as u16).into();
        ClientStatsDataPoint::convert_to_largest_data_unit(scalar * KILOBYTE)
    }

    /// Convert given data volume into largest divisible unit type of `byte`
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

    /// Format throughput units for Display
    fn format_throughput(throughput: u64) -> DataThroughput {
        #[cfg(not(target_arch = "wasm32"))]
        let scalar: AmountT = (throughput as u32).into();
        #[cfg(target_arch = "wasm32")]
        let scalar: AmountT = (throughput as u16).into();
        ClientStatsDataPoint::covert_to_largest_throughput_unit(scalar * BYTE_PER_SECOND)
    }

    /// Convert given data throughput into largest divisible unit type of `byte per second`
    /// Rate will always be with respect to unit per second
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

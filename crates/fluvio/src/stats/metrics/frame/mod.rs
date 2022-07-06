mod convert;

use crate::FluvioError;
use crate::stats::{
    ClientStatsDataCollect, ClientStatsMetric, ClientStatsMetricRaw, ClientStatsMetricFormat,
    unix_timestamp_nanos,
};
use serde::{Serialize, Deserialize};

use std::string::ToString;
use quantities::{prelude::*, AMNT_ONE};
use quantities::datathroughput::{
    DataThroughput, TERABYTE_PER_SECOND, GIGABYTE_PER_SECOND, MEGABYTE_PER_SECOND,
    KILOBYTE_PER_SECOND, BYTE_PER_SECOND,
};
use quantities::datavolume::{DataVolume, BYTE, KILOBYTE, MEGABYTE, GIGABYTE, TERABYTE};
use quantities::duration::{
    Duration as QuantDuration, MINUTE, SECOND, MILLISECOND, MICROSECOND, NANOSECOND,
};

/// This is a complete collection of client data being collected for the session:
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct ClientStatsDataFrame {
    /// Start time when struct was created
    /// This is Unix Epoch time, in nanoseconds
    start_time: i64,
    /// This is the elapsed time of when the frame was created, in nanoseconds
    run_time: i64,
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
    /// The throughput of the last transfer, in bytes per second
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
    /// Total time spent waiting for data transfer, in nanoseconds
    latency: u64,
    /// Last polled memory usage of client process, in kilobytes
    mem: u64,
    /// Total number of records processed
    records: u64,
    /// The throughput of current session, in bytes per second
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

impl ClientStatsDataFrame {
    pub fn get(&self, stat: ClientStatsMetric) -> ClientStatsMetricRaw {
        match stat {
            ClientStatsMetric::StartTime => ClientStatsMetricRaw::StartTime(self.start_time),
            ClientStatsMetric::RunTime => ClientStatsMetricRaw::RunTime(self.run_time),
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
        //println!("Bytes {} Throughput {}", self.bytes, self.throughput);
        match stat {
            ClientStatsMetric::StartTime => ClientStatsMetricFormat::StartTime(self.start_time),
            ClientStatsMetric::RunTime => ClientStatsMetricFormat::RunTime(
                Self::format_duration_from_nanos(self.run_time as u64),
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
            ClientStatsMetric::LastThroughput => ClientStatsMetricFormat::LastThroughput(
                Self::format_throughput(self.last_throughput),
            ),
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
                ClientStatsMetricFormat::Throughput(Self::format_throughput(self.throughput))
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

    /// Generates a CSV row. Columns returned based on client request using Vec<ClientStatsMetric>
    pub fn csv_dataframe(&self, stats: &Vec<ClientStatsMetric>) -> Result<String, FluvioError> {
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
        //println!("at fn call: {}", nanoseconds);
        #[cfg(not(target_arch = "wasm32"))]
        let scalar: AmountT = (nanoseconds as f64).into();
        #[cfg(target_arch = "wasm32")]
        let scalar: AmountT = (nanoseconds as f32).into();
        //println!("Pre format: {}", scalar);
        //println!("Pre convert: {}", (scalar * NANOSECOND));
        let convert = ClientStatsDataFrame::convert_to_largest_time_unit(scalar * NANOSECOND);
        //println!("Post format: {}", convert);
        convert
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
        ClientStatsDataFrame::convert_to_largest_data_unit(scalar * BYTE)
    }

    /// Format memory units for Display
    fn format_memory(mem: u64) -> DataVolume {
        #[cfg(not(target_arch = "wasm32"))]
        let scalar: AmountT = (mem as u32).into();
        #[cfg(target_arch = "wasm32")]
        let scalar: AmountT = (mem as u16).into();
        ClientStatsDataFrame::convert_to_largest_data_unit(scalar * KILOBYTE)
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
        ClientStatsDataFrame::covert_to_largest_throughput_unit(scalar * BYTE_PER_SECOND)
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

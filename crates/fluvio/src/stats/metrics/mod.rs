mod raw;
pub use raw::ClientStatsMetricRaw;
mod format;
pub use format::ClientStatsMetricFormat;
mod frame;
pub use frame::ClientStatsDataFrame;
mod histogram;
pub use histogram::ClientStatsHistogram;

use strum::{EnumIter, Display};

// Used as a selector
#[derive(Debug, Clone, Copy, EnumIter, Display)]
pub enum ClientStatsMetric {
    #[strum(serialize = "run_time")]
    RunTime,
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

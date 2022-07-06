use strum::{Display, EnumIter};

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

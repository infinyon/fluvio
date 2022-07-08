use serde::{Deserialize, Serialize};
use strum::IntoEnumIterator;

use super::ClientStatsMetric;

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
            Self::Data => {
                vec![
                    ClientStatsMetric::LastUpdated,
                    ClientStatsMetric::RunTime,
                    ClientStatsMetric::Offset,
                    ClientStatsMetric::LastBatches,
                    ClientStatsMetric::LastRecords,
                    ClientStatsMetric::LastBytes,
                    ClientStatsMetric::LastLatency,
                    ClientStatsMetric::LastThroughput,
                    ClientStatsMetric::Batches,
                    ClientStatsMetric::Records,
                    ClientStatsMetric::Bytes,
                    ClientStatsMetric::Latency,
                    ClientStatsMetric::Throughput,
                    ClientStatsMetric::SecondBatches,
                    ClientStatsMetric::SecondRecords,
                    ClientStatsMetric::SecondLatency,
                    ClientStatsMetric::SecondThroughput,
                    ClientStatsMetric::SecondMeanLatency,
                    ClientStatsMetric::SecondMeanThroughput,
                    ClientStatsMetric::MaxThroughput,
                    ClientStatsMetric::MeanThroughput,
                    ClientStatsMetric::MeanLatency,
                    ClientStatsMetric::StdDevLatency,
                    ClientStatsMetric::P50Latency,
                    ClientStatsMetric::P90Latency,
                    ClientStatsMetric::P99Latency,
                    ClientStatsMetric::P999Latency,
                ]
            }
            Self::System => {
                vec![
                    ClientStatsMetric::LastUpdated,
                    ClientStatsMetric::RunTime,
                    ClientStatsMetric::Pid,
                    ClientStatsMetric::Cpu,
                    ClientStatsMetric::Mem,
                ]
            }
        }
    }
}

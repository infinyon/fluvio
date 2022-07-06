use hdrhistogram::{Histogram, CreationError};
use crate::error::{Result, FluvioError};
use super::{ClientStatsMetric, ClientStatsMetricRaw};
use std::collections::VecDeque;
use super::ClientStatsDataPoint;

#[derive(Debug, Default)]
pub struct ClientStatsHistogram {
    second_window: VecDeque<ClientStatsDataPoint>,
    second_marked: bool,
    total: VecDeque<ClientStatsDataPoint>,
}

struct LoadedHistograms {
    pub latency: Histogram<u64>,
    pub bytes_sent: Histogram<u64>,
    pub throughput: Histogram<u64>,
    pub records: Histogram<u64>,
}

impl LoadedHistograms {
    fn new() -> Result<Self, CreationError> {
        Ok(Self {
            bytes_sent: Histogram::<u64>::new(2)?,
            latency: Histogram::<u64>::new(2)?,
            throughput: Histogram::<u64>::new(2)?,
            records: Histogram::<u64>::new(2)?,
        })
    }
}

impl ClientStatsHistogram {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&mut self, datapoint: ClientStatsDataPoint) {
        // Set the max length of the queue when we mark the second
        if !self.second_marked {
            self.second_window.push_back(datapoint);
        } else {
            self.second_window.push_back(datapoint);
            let _ = self.second_window.pop_front();
        }

        let _ = self.total.push_back(datapoint);
    }

    // this is maybe a hack way to achieve a sliding window
    pub fn mark_second(&mut self) {
        self.second_marked = true
    }

    pub fn get_agg_stats_update(&self) -> Result<Vec<ClientStatsMetricRaw>, FluvioError> {
        let second_frame = ClientStatsHistogram::load_histogram(&self.second_window)?;
        let total_frame = ClientStatsHistogram::load_histogram(&self.total)?;

        let mut latency_per_second = 0;
        let mut bytes_per_second = 0; // AKA Throughput
        let mut records_per_second = 0;
        let mut batches_per_second = 0;

        for n in &self.second_window {
            latency_per_second += n.get(ClientStatsMetric::LastLatency).as_u64();
            bytes_per_second += n.get(ClientStatsMetric::LastBytes).as_u64();
            records_per_second += n.get(ClientStatsMetric::LastRecords).as_u64();
            batches_per_second += n.get(ClientStatsMetric::LastBatches).as_u64();
        }

        let mean_latency_per_second = second_frame.latency.mean();
        let mean_throughput_per_second = second_frame.throughput.mean();

        let mean_latency = total_frame.latency.mean();
        let mean_throughput = total_frame.throughput.mean();
        let max_throughput = total_frame.throughput.max();
        let std_dev_latency = total_frame.latency.stdev();
        let p50_latency = total_frame.latency.value_at_percentile(0.50);
        let p90_latency = total_frame.latency.value_at_percentile(0.90);
        let p99_latency = total_frame.latency.value_at_percentile(0.99);
        let p999_latency = total_frame.latency.value_at_percentile(0.999);

        let aggregate_stats = vec![
            ClientStatsMetricRaw::SecondLatency(latency_per_second),
            ClientStatsMetricRaw::SecondThroughput(bytes_per_second),
            ClientStatsMetricRaw::SecondRecords(records_per_second),
            ClientStatsMetricRaw::SecondBatches(batches_per_second),
            ClientStatsMetricRaw::SecondMeanLatency(mean_latency_per_second as u64),
            ClientStatsMetricRaw::SecondMeanThroughput(mean_throughput_per_second as u64),
            ClientStatsMetricRaw::MeanLatency(mean_latency as u64),
            ClientStatsMetricRaw::MeanThroughput(mean_throughput as u64),
            ClientStatsMetricRaw::MaxThroughput(max_throughput as u64),
            ClientStatsMetricRaw::StdDevLatency(std_dev_latency as u64),
            ClientStatsMetricRaw::P50Latency(p50_latency),
            ClientStatsMetricRaw::P90Latency(p90_latency),
            ClientStatsMetricRaw::P99Latency(p99_latency),
            ClientStatsMetricRaw::P999Latency(p999_latency),
        ];

        Ok(aggregate_stats)
    }

    fn load_histogram(
        data: &VecDeque<ClientStatsDataPoint>,
    ) -> Result<LoadedHistograms, FluvioError> {
        let mut histogram = LoadedHistograms::new().map_err(|_| {
            FluvioError::Other("Problem adding datapoint into histogram".to_string())
        })?;

        let _: Vec<_> = data
            .iter()
            .map(|n| {
                let last_latency = if let ClientStatsMetricRaw::LastLatency(l) =
                    n.get(ClientStatsMetric::LastLatency)
                {
                    l
                } else {
                    0
                };
                let last_bytes = if let ClientStatsMetricRaw::LastBytes(b) =
                    n.get(ClientStatsMetric::LastBytes)
                {
                    b
                } else {
                    0
                };
                let last_throughput = if let ClientStatsMetricRaw::LastThroughput(t) =
                    n.get(ClientStatsMetric::LastThroughput)
                {
                    t
                } else {
                    0
                };
                let last_records = if let ClientStatsMetricRaw::LastRecords(r) =
                    n.get(ClientStatsMetric::LastRecords)
                {
                    r
                } else {
                    0
                };

                vec![
                    histogram.latency.record(last_latency as u64).map_err(|_| {
                        FluvioError::Other("Problem adding datapoint into histogram".to_string())
                    }),
                    histogram.bytes_sent.record(last_bytes as u64).map_err(|_| {
                        FluvioError::Other("Problem adding datapoint into histogram".to_string())
                    }),
                    histogram
                        .throughput
                        .record(last_throughput as u64)
                        .map_err(|_| {
                            FluvioError::Other(
                                "Problem adding datapoint into histogram".to_string(),
                            )
                        }),
                    histogram.records.record(last_records).map_err(|_| {
                        FluvioError::Other("Problem adding datapoint into histogram".to_string())
                    }),
                ]
            })
            .map(|f| {
                f.into_iter()
                    .map(|x| {
                        let _ = x.as_ref().ok();
                    })
                    .collect::<Vec<()>>()
            })
            .collect();

        Ok(histogram)
    }
}

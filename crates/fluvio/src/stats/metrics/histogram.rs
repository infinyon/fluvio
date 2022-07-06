use hdrhistogram::{Histogram, CreationError};
use crate::error::{Result, FluvioError};
use super::{ClientStatsMetric, ClientStatsMetricRaw, ClientStatsDataFrame};
use std::collections::VecDeque;

#[derive(Debug, Default)]
pub struct ClientStatsHistogram {
    second_window: VecDeque<ClientStatsDataFrame>,
    second_marked: bool,
    total: VecDeque<ClientStatsDataFrame>,
}

struct LoadedHistograms {
    pub latency: Histogram<u64>,
    pub bytes_transferred: Histogram<u64>,
    pub throughput: Histogram<u64>,
    pub records: Histogram<u64>,
}

impl LoadedHistograms {
    fn new() -> Result<Self, CreationError> {
        Ok(Self {
            bytes_transferred: Histogram::<u64>::new(2)?,
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

    pub fn record(&mut self, dataframe: ClientStatsDataFrame) {
        // Set the max length of the queue when we mark the second
        if !self.second_marked {
            self.second_window.push_back(dataframe);
        } else {
            self.second_window.push_back(dataframe);
            let _ = self.second_window.pop_front();
        }

        let _ = self.total.push_back(dataframe);
    }

    // this is maybe a hack way to achieve a sliding window
    pub fn tick(&mut self) {
        if !self.second_marked {
            self.second_marked = true
        }
    }

    pub fn get_agg_stats_update(&self) -> Result<Vec<ClientStatsMetricRaw>, FluvioError> {
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

        let mut total_latency = 0;
        let mut total_throughput = 0;

        for n in &self.total {
            total_latency += n.get(ClientStatsMetric::LastLatency).as_u64();
            total_throughput += n.get(ClientStatsMetric::LastThroughput).as_u64();
        }

        let mean_latency_per_second = latency_per_second / self.second_window.len() as u64;
        let mean_throughput_per_second = bytes_per_second / self.second_window.len() as u64;

        let total_frame = ClientStatsHistogram::load_histogram(&self.total)?;

        let mean_latency = (total_latency as f64 / self.total.len() as f64) as u64;
        let mean_throughput = total_throughput / self.total.len() as u64;
        let mut max_throughput = 0;

        if let Some(m) = &self
            .total
            .clone()
            .into_iter()
            .map(|x| x.get(ClientStatsMetric::Throughput).as_u64())
            .max()
        {
            max_throughput = *m;
        }

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
        data: &VecDeque<ClientStatsDataFrame>,
    ) -> Result<LoadedHistograms, FluvioError> {
        let mut histogram = LoadedHistograms::new().map_err(|_| {
            FluvioError::Other("Problem adding dataframe into histogram".to_string())
        })?;

        for n in data {
            let last_latency = if let ClientStatsMetricRaw::LastLatency(l) =
                n.get(ClientStatsMetric::LastLatency)
            {
                l
            } else {
                0
            };
            let last_bytes =
                if let ClientStatsMetricRaw::LastBytes(b) = n.get(ClientStatsMetric::LastBytes) {
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

            // Load the data into their respective histogram
            histogram.latency.record(last_latency).map_err(|_| {
                FluvioError::Other("Problem adding latency into histogram".to_string())
            })?;
            histogram
                .bytes_transferred
                .record(last_bytes)
                .map_err(|_| {
                    FluvioError::Other("Problem adding bytes into histogram".to_string())
                })?;
            histogram.throughput.record(last_throughput).map_err(|_| {
                FluvioError::Other("Problem adding throughput into histogram".to_string())
            })?;
            histogram.records.record(last_records).map_err(|_| {
                FluvioError::Other("Problem adding records into histogram".to_string())
            })?;
        }
        Ok(histogram)
    }
}

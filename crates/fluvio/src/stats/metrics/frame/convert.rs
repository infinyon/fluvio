use crate::stats::{ClientStats, ClientStatsMetricRaw, ClientStatsDataFrame, ClientStatsMetric};
use strum::IntoEnumIterator;

/// This copies from `ClientStats` passes values through to its respective field.
/// The exceptions to value pass-through are:
/// * CPU - Due to sample collection method, value shifted to include decimal points. This now a human-readable percentage.  
impl From<&ClientStats> for ClientStatsDataFrame {
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
                ClientStatsMetric::RunTime => {
                    let run_time = if let ClientStatsMetricRaw::RunTime(run_time) =
                        current.get(ClientStatsMetric::RunTime)
                    {
                        run_time
                    } else {
                        0
                    };

                    //println!("run_time raw: {run_time}");

                    data_point.run_time = run_time;
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

                    //#[cfg(not(target_arch = "wasm32"))]
                    //let bytes_per_second = (last_throughput as f64 * NANOSECOND.scale()) as u64;
                    //#[cfg(target_arch = "wasm32")]
                    //let bytes_per_second = (last_throughput as f32 * NANOSECOND.scale()) as u64;

                    //data_point.last_throughput = bytes_per_second;
                    data_point.last_throughput = last_throughput;
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
                    let latency = if let ClientStatsMetricRaw::Latency(latency) =
                        current.get(ClientStatsMetric::Latency)
                    {
                        latency
                    } else {
                        0
                    };
                    data_point.latency = latency;
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

                    data_point.throughput = throughput;
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

                    //#[cfg(not(target_arch = "wasm32"))]
                    //let bytes_per_second = (mean_throughput as f64 * NANOSECOND.scale()) as u64;
                    //#[cfg(target_arch = "wasm32")]
                    //let bytes_per_second = (mean_throughput as f32 * NANOSECOND.scale()) as u64;

                    data_point.mean_throughput = mean_throughput;
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

#[cfg(test)]
mod test {
    use crate::stats::{
        ClientStats, ClientStatsMetric, ClientStatsMetricRaw, ClientStatsDataCollect,
        ClientStatsUpdateBuilder,
    };
    use crate::FluvioError;
    use rand::prelude::*;
    use strum::IntoEnumIterator;

    #[test]
    fn test_convert() -> Result<(), FluvioError> {
        let mut metrics = Vec::new();
        let mut rng = rand::thread_rng();

        // Randomize the stats
        for m in ClientStatsMetric::iter() {
            let metric = match m {
                ClientStatsMetric::StartTime => ClientStatsMetricRaw::StartTime(rng.gen()),
                ClientStatsMetric::RunTime => ClientStatsMetricRaw::RunTime(rng.gen()),
                ClientStatsMetric::Pid => ClientStatsMetricRaw::Pid(rng.gen()),
                ClientStatsMetric::Offset => ClientStatsMetricRaw::Offset(rng.gen()),
                ClientStatsMetric::LastBatches => ClientStatsMetricRaw::LastBatches(rng.gen()),
                ClientStatsMetric::LastBytes => ClientStatsMetricRaw::LastBytes(rng.gen()),
                ClientStatsMetric::LastLatency => ClientStatsMetricRaw::LastLatency(rng.gen()),
                ClientStatsMetric::LastRecords => ClientStatsMetricRaw::LastRecords(rng.gen()),
                ClientStatsMetric::LastThroughput => {
                    ClientStatsMetricRaw::LastThroughput(rng.gen())
                }
                ClientStatsMetric::LastUpdated => ClientStatsMetricRaw::LastUpdated(rng.gen()),
                ClientStatsMetric::Batches => ClientStatsMetricRaw::Batches(rng.gen()),
                ClientStatsMetric::Bytes => ClientStatsMetricRaw::Bytes(rng.gen()),
                ClientStatsMetric::Cpu => ClientStatsMetricRaw::Cpu(rng.gen()),
                ClientStatsMetric::Mem => ClientStatsMetricRaw::Mem(rng.gen()),
                ClientStatsMetric::Latency => ClientStatsMetricRaw::Latency(rng.gen()),
                ClientStatsMetric::Records => ClientStatsMetricRaw::Records(rng.gen()),
                ClientStatsMetric::Throughput => ClientStatsMetricRaw::Throughput(rng.gen()),
                ClientStatsMetric::SecondBatches => ClientStatsMetricRaw::SecondBatches(rng.gen()),
                ClientStatsMetric::SecondLatency => ClientStatsMetricRaw::SecondLatency(rng.gen()),
                ClientStatsMetric::SecondRecords => ClientStatsMetricRaw::SecondRecords(rng.gen()),
                ClientStatsMetric::SecondThroughput => {
                    ClientStatsMetricRaw::SecondThroughput(rng.gen())
                }
                ClientStatsMetric::SecondMeanLatency => {
                    ClientStatsMetricRaw::SecondMeanLatency(rng.gen())
                }
                ClientStatsMetric::SecondMeanThroughput => {
                    ClientStatsMetricRaw::SecondMeanThroughput(rng.gen())
                }
                ClientStatsMetric::MaxThroughput => ClientStatsMetricRaw::MaxThroughput(rng.gen()),
                ClientStatsMetric::MeanThroughput => {
                    ClientStatsMetricRaw::MeanThroughput(rng.gen())
                }
                ClientStatsMetric::MeanLatency => ClientStatsMetricRaw::MeanLatency(rng.gen()),
                ClientStatsMetric::StdDevLatency => ClientStatsMetricRaw::StdDevLatency(rng.gen()),
                ClientStatsMetric::P50Latency => ClientStatsMetricRaw::P50Latency(rng.gen()),
                ClientStatsMetric::P90Latency => ClientStatsMetricRaw::P90Latency(rng.gen()),
                ClientStatsMetric::P99Latency => ClientStatsMetricRaw::P99Latency(rng.gen()),
                ClientStatsMetric::P999Latency => ClientStatsMetricRaw::P999Latency(rng.gen()),
            };

            metrics.push(metric);
        }

        // Choosing None so Cpu and Mem values will not update
        let stats_collect = ClientStatsDataCollect::None;

        let client_stats = ClientStats::new(stats_collect);

        client_stats.update(ClientStatsUpdateBuilder { data: metrics })?;

        let frame = client_stats.get_dataframe();

        // Verify dataframe matches client
        for metric in ClientStatsMetric::iter() {
            match metric {
                ClientStatsMetric::StartTime => {
                    let start_time = if let ClientStatsMetricRaw::StartTime(start_time) =
                        client_stats.get(ClientStatsMetric::StartTime)
                    {
                        start_time
                    } else {
                        0
                    };
                    assert_eq!(frame.start_time, start_time);
                }
                ClientStatsMetric::RunTime => {
                    let run_time = if let ClientStatsMetricRaw::RunTime(run_time) =
                        client_stats.get(ClientStatsMetric::RunTime)
                    {
                        run_time
                    } else {
                        0
                    };
                    assert!(frame.run_time < run_time);
                }
                ClientStatsMetric::Pid => {
                    let pid = if let ClientStatsMetricRaw::Pid(pid) =
                        client_stats.get(ClientStatsMetric::Pid)
                    {
                        pid
                    } else {
                        0
                    };
                    assert_eq!(frame.pid, pid);
                }
                ClientStatsMetric::Offset => {
                    let offset = if let ClientStatsMetricRaw::Offset(offset) =
                        client_stats.get(ClientStatsMetric::Offset)
                    {
                        offset
                    } else {
                        0
                    };
                    assert_eq!(frame.offset, offset);
                }
                ClientStatsMetric::LastBatches => {
                    let last_batches = if let ClientStatsMetricRaw::LastBatches(last_batches) =
                        client_stats.get(ClientStatsMetric::LastBatches)
                    {
                        last_batches
                    } else {
                        0
                    };
                    assert_eq!(frame.last_batches, last_batches);
                }
                ClientStatsMetric::LastBytes => {
                    let last_bytes = if let ClientStatsMetricRaw::LastBytes(last_bytes) =
                        client_stats.get(ClientStatsMetric::LastBytes)
                    {
                        last_bytes
                    } else {
                        0
                    };
                    assert_eq!(frame.last_bytes, last_bytes);
                }
                ClientStatsMetric::LastLatency => {
                    let last_latency = if let ClientStatsMetricRaw::LastLatency(last_latency) =
                        client_stats.get(ClientStatsMetric::LastLatency)
                    {
                        last_latency
                    } else {
                        0
                    };
                    assert_eq!(frame.last_latency, last_latency);
                }
                ClientStatsMetric::LastRecords => {
                    let last_records = if let ClientStatsMetricRaw::LastRecords(last_records) =
                        client_stats.get(ClientStatsMetric::LastRecords)
                    {
                        last_records
                    } else {
                        0
                    };
                    assert_eq!(frame.last_records, last_records);
                }
                ClientStatsMetric::LastThroughput => {
                    let last_throughput =
                        if let ClientStatsMetricRaw::LastThroughput(last_throughput) =
                            client_stats.get(ClientStatsMetric::LastThroughput)
                        {
                            last_throughput
                        } else {
                            0
                        };
                    assert_eq!(frame.last_throughput, last_throughput);
                }
                ClientStatsMetric::LastUpdated => {
                    let last_updated = if let ClientStatsMetricRaw::LastUpdated(last_updated) =
                        client_stats.get(ClientStatsMetric::LastUpdated)
                    {
                        last_updated
                    } else {
                        0
                    };
                    assert_eq!(frame.last_updated, last_updated);
                }
                ClientStatsMetric::Batches => {
                    let batches = if let ClientStatsMetricRaw::Batches(batches) =
                        client_stats.get(ClientStatsMetric::Batches)
                    {
                        batches
                    } else {
                        0
                    };
                    assert_eq!(frame.batches, batches);
                }
                ClientStatsMetric::Bytes => {
                    let bytes = if let ClientStatsMetricRaw::Bytes(bytes) =
                        client_stats.get(ClientStatsMetric::Bytes)
                    {
                        bytes
                    } else {
                        0
                    };
                    assert_eq!(frame.bytes, bytes);
                }
                ClientStatsMetric::Cpu => {
                    let cpu = if let ClientStatsMetricRaw::Cpu(cpu) =
                        client_stats.get(ClientStatsMetric::Cpu)
                    {
                        cpu
                    } else {
                        0
                    };
                    assert_eq!(frame.cpu, (cpu as f32 / 1_000.0));
                }
                ClientStatsMetric::Mem => {
                    let mem = if let ClientStatsMetricRaw::Mem(mem) =
                        client_stats.get(ClientStatsMetric::Mem)
                    {
                        mem
                    } else {
                        0
                    };
                    assert_eq!(frame.mem, mem);
                }
                ClientStatsMetric::Latency => {
                    let latency = if let ClientStatsMetricRaw::Latency(latency) =
                        client_stats.get(ClientStatsMetric::Latency)
                    {
                        latency
                    } else {
                        0
                    };
                    assert_eq!(frame.latency, latency);
                }
                ClientStatsMetric::Records => {
                    let records = if let ClientStatsMetricRaw::Records(records) =
                        client_stats.get(ClientStatsMetric::Records)
                    {
                        records
                    } else {
                        0
                    };
                    assert_eq!(frame.records, records);
                }
                ClientStatsMetric::Throughput => {
                    let throughput = if let ClientStatsMetricRaw::Throughput(throughput) =
                        client_stats.get(ClientStatsMetric::Throughput)
                    {
                        throughput
                    } else {
                        0
                    };
                    assert_eq!(frame.throughput, throughput);
                }
                ClientStatsMetric::SecondBatches => {
                    let second_batches =
                        if let ClientStatsMetricRaw::SecondBatches(second_batches) =
                            client_stats.get(ClientStatsMetric::SecondBatches)
                        {
                            second_batches
                        } else {
                            0
                        };
                    assert_eq!(frame.second_batches, second_batches);
                }
                ClientStatsMetric::SecondLatency => {
                    let second_latency =
                        if let ClientStatsMetricRaw::SecondLatency(second_latency) =
                            client_stats.get(ClientStatsMetric::SecondLatency)
                        {
                            second_latency
                        } else {
                            0
                        };
                    assert_eq!(frame.second_latency, second_latency);
                }
                ClientStatsMetric::SecondRecords => {
                    let second_records =
                        if let ClientStatsMetricRaw::SecondRecords(second_records) =
                            client_stats.get(ClientStatsMetric::SecondRecords)
                        {
                            second_records
                        } else {
                            0
                        };
                    assert_eq!(frame.second_records, second_records);
                }
                ClientStatsMetric::SecondThroughput => {
                    let second_throughput =
                        if let ClientStatsMetricRaw::SecondThroughput(second_throughput) =
                            client_stats.get(ClientStatsMetric::SecondThroughput)
                        {
                            second_throughput
                        } else {
                            0
                        };
                    assert_eq!(frame.second_throughput, second_throughput);
                }
                ClientStatsMetric::SecondMeanLatency => {
                    let second_mean_latency =
                        if let ClientStatsMetricRaw::SecondMeanLatency(second_mean_latency) =
                            client_stats.get(ClientStatsMetric::SecondMeanLatency)
                        {
                            second_mean_latency
                        } else {
                            0
                        };
                    assert_eq!(frame.second_mean_latency, second_mean_latency);
                }
                ClientStatsMetric::SecondMeanThroughput => {
                    let second_mean_throughput =
                        if let ClientStatsMetricRaw::SecondMeanThroughput(second_mean_throughput) =
                            client_stats.get(ClientStatsMetric::SecondMeanThroughput)
                        {
                            second_mean_throughput
                        } else {
                            0
                        };
                    assert_eq!(frame.second_mean_throughput, second_mean_throughput);
                }
                ClientStatsMetric::MaxThroughput => {
                    let max_throughput =
                        if let ClientStatsMetricRaw::MaxThroughput(max_throughput) =
                            client_stats.get(ClientStatsMetric::MaxThroughput)
                        {
                            max_throughput
                        } else {
                            0
                        };
                    assert_eq!(frame.max_throughput, max_throughput);
                }
                ClientStatsMetric::MeanThroughput => {
                    let mean_throughput =
                        if let ClientStatsMetricRaw::MeanThroughput(mean_throughput) =
                            client_stats.get(ClientStatsMetric::MeanThroughput)
                        {
                            mean_throughput
                        } else {
                            0
                        };
                    assert_eq!(frame.mean_throughput, mean_throughput);
                }
                ClientStatsMetric::MeanLatency => {
                    let mean_latency = if let ClientStatsMetricRaw::MeanLatency(mean_latency) =
                        client_stats.get(ClientStatsMetric::MeanLatency)
                    {
                        mean_latency
                    } else {
                        0
                    };
                    assert_eq!(frame.mean_latency, mean_latency);
                }
                ClientStatsMetric::StdDevLatency => {
                    let std_dev_latency =
                        if let ClientStatsMetricRaw::StdDevLatency(std_dev_latency) =
                            client_stats.get(ClientStatsMetric::StdDevLatency)
                        {
                            std_dev_latency
                        } else {
                            0
                        };
                    assert_eq!(frame.std_dev_latency, std_dev_latency);
                }
                ClientStatsMetric::P50Latency => {
                    let p50 = if let ClientStatsMetricRaw::P50Latency(p50) =
                        client_stats.get(ClientStatsMetric::P50Latency)
                    {
                        p50
                    } else {
                        0
                    };
                    assert_eq!(frame.p50_latency, p50);
                }
                ClientStatsMetric::P90Latency => {
                    let p90 = if let ClientStatsMetricRaw::P90Latency(p90) =
                        client_stats.get(ClientStatsMetric::P90Latency)
                    {
                        p90
                    } else {
                        0
                    };
                    assert_eq!(frame.p90_latency, p90);
                }
                ClientStatsMetric::P99Latency => {
                    let p99 = if let ClientStatsMetricRaw::P99Latency(p99) =
                        client_stats.get(ClientStatsMetric::P99Latency)
                    {
                        p99
                    } else {
                        0
                    };
                    assert_eq!(frame.p99_latency, p99);
                }
                ClientStatsMetric::P999Latency => {
                    let p999 = if let ClientStatsMetricRaw::P999Latency(p999) =
                        client_stats.get(ClientStatsMetric::P999Latency)
                    {
                        p999
                    } else {
                        0
                    };
                    assert_eq!(frame.p999_latency, p999);
                }
            };
        }

        Ok(())
    }
}

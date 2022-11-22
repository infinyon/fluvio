use std::time::{Duration, Instant};

use hdrhistogram::Histogram;

use crate::stats_collector::BatchStats;

pub fn compute_stats(data: &BatchStats) {
    // 1us to 1min with 3 degrees of precision
    let mut latency_histogram: Histogram<u64> =
        Histogram::new_with_bounds(1, 1000 * 1000 * 60, 3).unwrap();

    let mut first_produce_time: Option<Instant> = None;
    let mut last_produce_time: Option<Instant> = None;
    let mut first_consume_time: Option<Instant> = None;
    let mut last_consume_time: Option<Instant> = None; // TODO this is just the first time a single message was received... what should behavior be when multiple consumers
    let mut num_records = 0;
    let mut num_bytes = 0;
    for record in data.iter() {
        // TODO first or last recv time?
        latency_histogram += record.first_recv_latency().as_micros() as u64;
        let produced_time = record.send_time.unwrap();
        let consumed_time = record.first_received_time.unwrap();
        if let Some(p) = first_produce_time {
            if produced_time < p {
                first_produce_time = Some(produced_time);
            }
        } else {
            first_produce_time = Some(produced_time);
        };
        if let Some(p) = last_produce_time {
            if produced_time > p {
                last_produce_time = Some(produced_time);
            }
        } else {
            last_produce_time = Some(produced_time);
        };
        if let Some(c) = first_consume_time {
            if consumed_time < c {
                first_consume_time = Some(consumed_time);
            }
        } else {
            first_consume_time = Some(consumed_time);
        };
        if let Some(c) = last_consume_time {
            if consumed_time > c {
                last_consume_time = Some(consumed_time);
            }
        } else {
            last_consume_time = Some(consumed_time);
        };
        num_records += 1;
        num_bytes += record.num_bytes.unwrap();
    }
    let produce_time = last_produce_time.unwrap() - first_produce_time.unwrap();
    let consume_time = last_consume_time.unwrap() - first_consume_time.unwrap();
    let combined_time = last_consume_time.unwrap() - first_produce_time.unwrap();

    println!(
        "Produced {num_records} records totaling {:9.3} mb",
        num_bytes as f64 / 1000000.0
    );
    println!("Produce time:  {produce_time:?}");
    println!("Consume time:  {consume_time:?}");
    println!("Combined time: {combined_time:?}");

    println!(
        "Produce throughput:  {:9.3} mb/s",
        num_bytes as f64 / produce_time.as_secs_f64() / 1000000.0
    );
    println!(
        "Consume throughput:  {:9.3} mb/s",
        num_bytes as f64 / consume_time.as_secs_f64() / 1000000.0
    );
    println!(
        "Combined throughput: {:9.3} mb/s",
        num_bytes as f64 / combined_time.as_secs_f64() / 1000000.0
    );
    for quantile in vec![0.9, 0.99, 0.999] {
        println!(
            "Quantile: {:6.3} Latency: {:?}",
            quantile,
            Duration::from_micros(latency_histogram.value_at_quantile(quantile))
        );
    }
}

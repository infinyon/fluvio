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

    for v in latency_histogram.iter_quantiles(1) {
        println!(
            "Quantile: {:6.4} Latency: {:?}",
            v.quantile_iterated_to(),
            Duration::from_micros(v.value_iterated_to())
        );
    }

    println!("Produced {num_records} records totaling {num_bytes} bytes");
    println!(
        "Produce time: {:?} Consume time: {:?} Combined {:?}",
        produce_time, consume_time, combined_time
    );
    println!(
        "produce  throughput bytes   / sec: {:7.2}",
        num_bytes as f64 / produce_time.as_secs_f64()
    );
    println!(
        "consume  throughput bytes   / sec: {:7.2}",
        num_bytes as f64 / consume_time.as_secs_f64()
    );
    println!(
        "combined throughput bytes   / sec: {:7.2}",
        num_bytes as f64 / combined_time.as_secs_f64()
    );
    println!(
        "produce  throughput records / sec: {:7.2}",
        num_records as f64 / produce_time.as_secs_f64()
    );
    println!(
        "consume  throughput records / sec: {:7.2}",
        num_records as f64 / consume_time.as_secs_f64()
    );
    println!(
        "combined throughput records / sec: {:7.2}",
        num_records as f64 / combined_time.as_secs_f64()
    );
}

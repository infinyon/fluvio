use std::{
    time::{Duration, Instant},
    collections::HashMap,
    fmt::{Formatter, Display},
};

use hdrhistogram::Histogram;
use log::{info, trace};
use statrs::distribution::{StudentsT, ContinuousCDF};

use crate::stats_collector::BatchStats;
use serde::{Serialize, Deserialize};

pub const P_VALUE: f64 = 0.001;
// Used to compare if two p_values are equal in TTestResult
const P_VALUE_EPSILON: f64 = 0.00005;
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

pub struct BenchmarkStats {
    data: HashMap<Variable, Vec<f64>>,
}

impl BenchmarkStats {
    pub fn compare(&self, other: &BenchmarkStats) -> CompareResult {
        let mut better = false;
        let mut worse = false;
        for (key, value) in self.data.iter() {
            if let Some(other_value) = other.data.get(key) {
                let result = key.compare(value, other_value);
                info!("Compare {key:?} result: {result:?}");
                match result {
                    CompareResult::Better => better = true,
                    CompareResult::Worse => worse = true,
                    CompareResult::Mixed => unreachable!(),
                    CompareResult::NoChange => {}
                    CompareResult::Uncomparable => return CompareResult::Uncomparable,
                }
            } else {
                info!("Key not found: {key:?}");
                return CompareResult::Uncomparable;
            }
        }

        if better && worse {
            CompareResult::Mixed
        } else if better {
            CompareResult::Better
        } else if worse {
            CompareResult::Worse
        } else {
            CompareResult::NoChange
        }
    }
}

#[derive(Copy, Clone, Serialize, Deserialize, Debug, Hash, Eq, PartialEq)]
pub enum Variable {
    Q900,
    Q990,
    Q999,
    ProducerThroughput,
    ConsumerThroughput,
    CombinedThroughput,
}

impl Variable {
    pub fn compare(&self, a: &[f64], b: &[f64]) -> CompareResult {
        if a.len() != b.len() {
            return CompareResult::Uncomparable;
        }
        todo!()
    }
}

#[derive(Copy, Clone, Debug)]
pub enum CompareResult {
    /// At least one comparision was better at the p=0.001 level
    Better,
    /// At least one comparision was worse at the p=0.001 level
    Worse,
    /// At least one comparision was better and one comparision was worse at the p = 0.001 level
    Mixed,
    /// No comparisions were different at p = .001 level
    NoChange,
    /// The BenchmarkStats do not have the same variables so they cannot be compared
    Uncomparable,
}

pub fn two_sample_t_test(
    x1: f64,
    x2: f64,
    std_dev_1: f64,
    std_dev_2: f64,
    num_samples: u64,
    p_value: f64,
) -> TTestResult {
    // Welchs-t-test
    // https://en.wikipedia.org/wiki/Student%27s_t-test#Equal_sample_sizes_and_variance
    // https://en.wikipedia.org/wiki/Welch%27s_t-test
    // https://www.statology.org/welchs-t-test/

    let v1 = std_dev_1 * std_dev_1;
    let v2 = std_dev_2 * std_dev_2;
    let n = num_samples as f64;

    let t = (x1 - x2) / (v1 / n + v2 / n).sqrt();
    let df_numerator = (v1 / n + v2 / n).powi(2);
    let df_denominator = (v1 / n).powi(2) / (n - 1.0) + (v2 / n).powi(2) / (n - 1.0);
    let degrees_of_freedom = df_numerator / df_denominator;

    let students_t_dist =
        StudentsT::new(0.0, 1.0, degrees_of_freedom).expect("Failed to create students t dist");

    // https://www.omnicalculator.com/statistics/p-value
    // Left-tailed test: p-value = cdf(x)
    // Right-tailed test: p-value = 1 - cdf(x)
    // Two-tailed test: p-value = 2 * min{cdf(x) , 1 - cdf(x)}
    let left_tailed_p_value = students_t_dist.cdf(t);
    let right_tailed_p_value = 1.0 - students_t_dist.cdf(t);
    // let two_tailed_p_value = 2.0 * f64_min(left_tailed_p_value, right_tailed_p_value);

    trace!(
        "t: {:?} df: {:?}, lv {:?}, rv {:?}",
        t,
        degrees_of_freedom,
        left_tailed_p_value,
        right_tailed_p_value
    );
    if left_tailed_p_value <= p_value {
        TTestResult::X1LessThanX2(left_tailed_p_value)
    } else if right_tailed_p_value <= p_value {
        TTestResult::X1GreaterThanX2(right_tailed_p_value)
    } else {
        TTestResult::FailedToRejectH0
    }
}
fn almost_equal(a: f64, b: f64, epsilon: f64) -> bool {
    let difference = a - b;
    difference.abs() < epsilon
}

#[derive(Debug, Copy, Clone)]
pub enum TTestResult {
    /// Contains p_value of left-tailed students t test
    X1GreaterThanX2(f64),
    FailedToRejectH0,
    /// Contains p_value of right-tailed students t test
    X1LessThanX2(f64),
}

impl Eq for TTestResult {}
impl PartialEq for TTestResult {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::X1GreaterThanX2(l0), Self::X1GreaterThanX2(r0)) => {
                almost_equal(*l0, *r0, P_VALUE_EPSILON)
            }
            (Self::FailedToRejectH0, Self::FailedToRejectH0) => true,
            (Self::X1LessThanX2(l0), Self::X1LessThanX2(r0)) => {
                almost_equal(*l0, *r0, P_VALUE_EPSILON)
            }
            _ => false,
        }
    }
}

impl Display for TTestResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            TTestResult::X1GreaterThanX2(p) => ('+', p),
            TTestResult::FailedToRejectH0 => {
                return write!(f, "?");
            }
            TTestResult::X1LessThanX2(p) => ('-', p),
        };
        write!(f, "{} (p={:5.3})", s.0, s.1)
    }
}

#[cfg(test)]
mod tests {

    use crate::stats::TTestResult;

    use super::two_sample_t_test;

    #[test]
    fn test_two_sample_t_test() {
        // Test cases done using comparisons with results from wolframalpha
        // https://www.wolframalpha.com/input?i=t+test&assumption=%7B%22F%22%2C+%22TTest%22%2C+%22xbar%22%7D+-%3E%221%22&assumption=%7B%22F%22%2C+%22TTest%22%2C+%22mu0%22%7D+-%3E%220%22&assumption=%7B%22F%22%2C+%22TTest%22%2C+%22s%22%7D+-%3E%2210%22&assumption=%7B%22F%22%2C+%22TTest%22%2C+%22n%22%7D+-%3E%2250%22

        assert_eq!(
            two_sample_t_test(100.0, 105.0, 5.0, 7.0, 5, 0.05),
            TTestResult::FailedToRejectH0
        );

        assert_eq!(
            two_sample_t_test(100.0, 105.0, 5.0, 7.0, 25, 0.05),
            TTestResult::X1LessThanX2(0.002869)
        );
        assert_eq!(
            two_sample_t_test(100.0, 97.0, 5.0, 7.0, 50, 0.05),
            TTestResult::X1GreaterThanX2(0.007794)
        );
    }
}

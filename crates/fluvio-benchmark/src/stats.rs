use std::{
    time::{Instant, Duration},
    collections::{HashMap, BTreeMap},
    fmt::{Formatter, Display},
    sync::Arc,
};

use async_std::sync::Mutex;
use hdrhistogram::Histogram;
use log::{info, trace};
use statrs::distribution::{StudentsT, ContinuousCDF};
use statrs::statistics::Statistics;

use crate::{
    stats_collector::BatchStats, benchmark_config::benchmark_settings::BenchmarkSettings,
    BenchmarkError,
};
use serde::{Serialize, Deserialize};

pub const P_VALUE: f64 = 0.001;
// Used to compare if two p_values are equal in TTestResult
const P_VALUE_EPSILON: f64 = 0.00005;

const HIST_PRECISION: u8 = 3;

#[derive(Clone, Default)]
pub struct AllStats {
    mutex: Arc<Mutex<HashMap<BenchmarkSettings, BenchmarkStats>>>,
}

impl AllStats {
    pub async fn encode(&self) -> Vec<u8> {
        let guard = self.mutex.lock().await;
        bincode::serialize(&*guard).unwrap()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, BenchmarkError> {
        let decoded: HashMap<BenchmarkSettings, BenchmarkStats> = bincode::deserialize(bytes)
            .map_err(|_| {
                BenchmarkError::ErrorWithExplanation("Failed to deserialized".to_string())
            })?;
        Ok(Self {
            mutex: Arc::new(Mutex::new(decoded)),
        })
    }
    pub async fn compare_stats(&self, settings: &BenchmarkSettings, other: AllStats) {
        let guard = self.mutex.lock().await;
        let other = other.mutex.lock().await;
        let stats = guard.get(settings).unwrap();

        if let Some(other_stats) = other.get(settings) {
            println!("Previous results for settings found:",);
            stats.compare(other_stats, settings)
        }
    }

    pub async fn print_results(&self, settings: &BenchmarkSettings) {
        let guard = self.mutex.lock().await;
        if let Some(stats) = guard.get(settings) {
            let values = stats.data.get(&Variable::Latency).unwrap();
            let mut hist: Histogram<u64> = Histogram::new(HIST_PRECISION).unwrap();
            for v in values.iter() {
                hist += *v;
            }
            println!("Latency");
            for percentile in [0.0, 0.5, 0.95, 0.99, 1.0] {
                println!(
                    "p{percentile:4.2}: {}",
                    Variable::Latency.format(hist.value_at_quantile(percentile))
                );
            }

            for variable in [
                Variable::ProducerThroughput,
                Variable::ConsumerThroughput,
                Variable::CombinedThroughput,
            ] {
                let values = stats.data.get(&variable).unwrap();
                let mut hist: Histogram<u64> = Histogram::new(HIST_PRECISION).unwrap();
                for v in values.iter() {
                    hist += *v;
                }

                println!("{} Max, Median, Min", variable);
                for percentile in [1.0, 0.5, 0.0] {
                    println!(
                        "p{percentile:4.2}: {}",
                        variable.format(hist.value_at_quantile(percentile))
                    );
                }
            }
        } else {
            println!("Stats unavailable");
        }
    }

    pub async fn compute_stats(&self, settings: &BenchmarkSettings, data: &BatchStats) {
        let mut first_produce_time: Option<Instant> = None;
        let mut last_produce_time: Option<Instant> = None;
        let mut first_consume_time: Option<Instant> = None;
        let mut last_consume_time: Option<Instant> = None; // TODO this is just the first time a single message was received... what should behavior be when multiple consumers
        let mut num_bytes = 0;
        let mut latency = Vec::new();
        for record in data.iter() {
            latency.push(record.first_recv_latency().as_micros() as u64);
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
            num_bytes += record.num_bytes.unwrap();
        }
        let produce_time = last_produce_time.unwrap() - first_produce_time.unwrap();
        let consume_time = last_consume_time.unwrap() - first_consume_time.unwrap();
        let combined_time = last_consume_time.unwrap() - first_produce_time.unwrap();

        self.record_data(settings, Variable::Latency, latency).await;
        self.record_data(
            settings,
            Variable::ProducerThroughput,
            vec![(num_bytes as f64 / produce_time.as_secs_f64()) as u64],
        )
        .await;
        self.record_data(
            settings,
            Variable::ConsumerThroughput,
            vec![(num_bytes as f64 / consume_time.as_secs_f64()) as u64],
        )
        .await;
        self.record_data(
            settings,
            Variable::CombinedThroughput,
            vec![(num_bytes as f64 / combined_time.as_secs_f64()) as u64],
        )
        .await;
    }

    async fn record_data(
        &self,
        settings: &BenchmarkSettings,
        variable: Variable,
        mut values: Vec<u64>,
    ) {
        let mut guard = self.mutex.lock().await;
        let benchmark_stats = guard.entry(settings.clone()).or_default();
        let entry = benchmark_stats.data.entry(variable).or_default();
        entry.append(&mut values);
    }
}

#[derive(Default, Serialize, Deserialize)]
pub struct BenchmarkStats {
    data: BTreeMap<Variable, Vec<u64>>,
}

impl BenchmarkStats {
    pub fn compare(&self, other: &BenchmarkStats, settings: &BenchmarkSettings) {
        for (variable, samples) in self.data.iter() {
            if let Some(other_samples) = other.data.get(variable) {
                let (samples, other_samples) = if samples.len() == settings.num_samples {
                    let samples: Vec<f64> = samples.iter().map(|x| *x as f64).collect();
                    let other_samples: Vec<f64> = other_samples.iter().map(|x| *x as f64).collect();
                    (samples, other_samples)
                } else {
                    let items_per_sample = samples.len() / settings.num_samples as usize;
                    let samples: Vec<f64> = (0..settings.num_samples)
                        .map(|i| {
                            *samples[i * items_per_sample..(i + 1) * items_per_sample]
                                .iter()
                                .max()
                                .unwrap() as f64
                        })
                        .collect();
                    let other_samples: Vec<f64> = (0..settings.num_samples)
                        .map(|i| {
                            *other_samples[i * items_per_sample..(i + 1) * items_per_sample]
                                .iter()
                                .max()
                                .unwrap() as f64
                        })
                        .collect();
                    (samples, other_samples)
                };
                let result = variable.compare(&samples, &other_samples);
                println!("Comparing {variable}... {}", variable.format_result(result));
            } else {
                info!("Key not found: {variable}");
            }
        }
    }
}

#[derive(Copy, Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum Variable {
    Latency,
    ProducerThroughput,
    ConsumerThroughput,
    CombinedThroughput,
}
impl Display for Variable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Variable::Latency => write!(f, "latency"),
            Variable::ProducerThroughput => write!(f, "producer throughput"),
            Variable::ConsumerThroughput => write!(f, "consumer throughput"),
            Variable::CombinedThroughput => write!(f, "combined throughput"),
        }
    }
}

impl Variable {
    pub fn compare(&self, a: &[f64], b: &[f64]) -> CompareResult {
        if a.len() != b.len() {
            return CompareResult::Uncomparable;
        }
        let a_mean = a.mean();
        let b_mean = b.mean();

        match two_sample_t_test(a_mean, b_mean, a.std_dev(), b.std_dev(), a.len(), P_VALUE) {
            TTestResult::X1GreaterThanX2(p) => self.greater(a_mean, b_mean, p),
            TTestResult::FailedToRejectH0 => CompareResult::NoChange,
            TTestResult::X1LessThanX2(p) => self.less(a_mean, b_mean, p),
        }
    }

    fn format_result(&self, result: CompareResult) -> String {
        match result {
            CompareResult::Better {
                previous,
                next,
                p_value,
            } => format!(
                "better: {} -> {} (p={:7.5})",
                self.format(previous as u64),
                self.format(next as u64),
                p_value
            ),
            CompareResult::Worse {
                previous,
                next,
                p_value,
            } => format!(
                "worse: {} -> {} (p={:7.5})",
                self.format(previous as u64),
                self.format(next as u64),
                p_value
            ),
            CompareResult::NoChange => "no statistically significant change detected".to_string(),
            CompareResult::Uncomparable => "uncomparable".to_string(),
        }
    }

    fn greater(&self, a_mean: f64, b_mean: f64, p_value: f64) -> CompareResult {
        match self {
            Variable::Latency => CompareResult::Worse {
                previous: b_mean,
                next: a_mean,
                p_value,
            },
            Variable::ProducerThroughput => CompareResult::Better {
                previous: b_mean,
                next: a_mean,
                p_value,
            },
            Variable::ConsumerThroughput => CompareResult::Better {
                previous: b_mean,
                next: a_mean,
                p_value,
            },
            Variable::CombinedThroughput => CompareResult::Better {
                previous: b_mean,
                next: a_mean,
                p_value,
            },
        }
    }
    fn less(&self, a_mean: f64, b_mean: f64, p_value: f64) -> CompareResult {
        match self {
            Variable::Latency => CompareResult::Better {
                previous: b_mean,
                next: a_mean,
                p_value,
            },
            Variable::ProducerThroughput => CompareResult::Worse {
                previous: b_mean,
                next: a_mean,
                p_value,
            },
            Variable::ConsumerThroughput => CompareResult::Worse {
                previous: b_mean,
                next: a_mean,
                p_value,
            },
            Variable::CombinedThroughput => CompareResult::Worse {
                previous: b_mean,
                next: a_mean,
                p_value,
            },
        }
    }

    fn format(&self, v: u64) -> String {
        match self {
            Variable::Latency => format!("{:>9?}", Duration::from_micros(v)),
            Variable::ProducerThroughput => format!("{:9.3}mb/s", v as f64 / 1000000.0),
            Variable::ConsumerThroughput => format!("{:9.3}mb/s", v as f64 / 1000000.0),
            Variable::CombinedThroughput => format!("{:9.3}mb/s", v as f64 / 1000000.0),
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub enum CompareResult {
    /// At least one comparision was better at the p=0.001 level
    Better {
        previous: f64,
        next: f64,
        p_value: f64,
    },
    /// At least one comparision was worse at the p=0.001 level
    Worse {
        previous: f64,
        next: f64,
        p_value: f64,
    },
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
    num_samples: usize,
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

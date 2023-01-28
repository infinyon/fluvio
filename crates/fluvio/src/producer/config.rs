use std::fmt::{Debug, Display, Formatter};
use std::str::FromStr;
use std::time::Duration;

use derive_builder::Builder;

use fluvio_future::retry::{ExponentialBackoff, FibonacciBackoff, FixedDelay};
use fluvio_spu_schema::Isolation;
use fluvio_compression::Compression;
use serde::{Serialize, Deserialize};

use crate::producer::partitioning::{Partitioner, SiphashRoundRobinPartitioner};
#[cfg(feature = "stats")]
use crate::stats::ClientStatsDataCollect;

const DEFAULT_LINGER_MS: u64 = 100;
const DEFAULT_TIMEOUT_MS: u64 = 1500;
const DEFAULT_BATCH_SIZE_BYTES: usize = 16_384;
const DEFAULT_BATCH_QUEUE_SIZE: usize = 100;

const DEFAULT_RETRIES_TIMEOUT: Duration = Duration::from_secs(300);
const DEFAULT_INITIAL_DELAY: Duration = Duration::from_millis(20);
const DEFAULT_MAX_DELAY: Duration = Duration::from_secs(200);
const DEFAULT_MAX_RETRIES: usize = 4;

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE_BYTES
}

fn default_batch_queue_size() -> usize {
    DEFAULT_BATCH_QUEUE_SIZE
}

fn default_linger_duration() -> Duration {
    Duration::from_millis(DEFAULT_LINGER_MS)
}

fn default_partitioner() -> Box<dyn Partitioner + Send + Sync> {
    Box::new(SiphashRoundRobinPartitioner::new())
}

fn default_timeout() -> Duration {
    Duration::from_millis(DEFAULT_TIMEOUT_MS)
}

fn default_isolation() -> Isolation {
    Isolation::default()
}

#[cfg(feature = "stats")]
fn default_stats_collect() -> ClientStatsDataCollect {
    ClientStatsDataCollect::default()
}

fn default_delivery() -> DeliverySemantic {
    DeliverySemantic::default()
}

/// Options used to adjust the behavior of the Producer.
/// Create this struct with [`TopicProducerConfigBuilder`].
///
/// Create a producer with a custom config with [`crate::Fluvio::topic_producer_with_config()`].
#[derive(Builder)]
#[builder(pattern = "owned")]
pub struct TopicProducerConfig {
    /// Maximum amount of bytes accumulated by the records before sending the batch.
    #[builder(default = "default_batch_size()")]
    pub(crate) batch_size: usize,
    /// Maximum amount of batches waiting in the queue before sending to the SPU.
    #[builder(default = "default_batch_queue_size()")]
    pub(crate) batch_queue_size: usize,
    /// Time to wait before sending messages to the server.
    #[builder(default = "default_linger_duration()")]
    pub(crate) linger: Duration,
    /// Partitioner assigns the partition to each record that needs to be send
    #[builder(default = "default_partitioner()")]
    pub(crate) partitioner: Box<dyn Partitioner + Send + Sync>,

    /// Compression algorithm used by Fluvio producer to compress data.
    /// If there is a topic level compression and it is not compatible with this setting, the producer
    /// initialization will fail.
    #[builder(setter(into, strip_option), default)]
    pub(crate) compression: Option<Compression>,

    /// Max time duration that the server is allowed to process the batch.
    #[builder(default = "default_timeout()")]
    pub(crate) timeout: Duration,

    /// [`Isolation`] level that the producer must respect.
    /// [`Isolation::ReadCommitted`] waits for messages to be committed (replicated) before
    /// sending the response to the caller.
    /// [`Isolation::ReadUncommitted`] just waits for the leader to accept the message.
    #[builder(default = "default_isolation()")]
    pub(crate) isolation: Isolation,

    #[cfg(feature = "stats")]
    /// Collect resource and data transfer stats used by Fluvio producer
    #[builder(default = "default_stats_collect()")]
    pub(crate) stats_collect: ClientStatsDataCollect,

    /// Delivery guarantees that producer must respect.
    /// [`DeliverySemantic::AtMostOnce`] - send records without waiting from response. `Fire and forget`
    /// approach.
    /// [`DeliverySemantic::AtLeastOnce`] - send records, wait for the response and retry
    /// if error occurred. Retry parameters, such as delay, retry strategy, timeout, etc.,
    /// can be configured in [`RetryPolicy`].
    #[builder(default = "default_delivery()")]
    pub(crate) delivery_semantic: DeliverySemantic,
}

impl Default for TopicProducerConfig {
    fn default() -> Self {
        Self {
            linger: default_linger_duration(),
            batch_size: default_batch_size(),
            batch_queue_size: default_batch_queue_size(),
            partitioner: default_partitioner(),
            compression: None,
            timeout: default_timeout(),
            isolation: default_isolation(),

            #[cfg(feature = "stats")]
            stats_collect: default_stats_collect(),
            delivery_semantic: default_delivery(),
        }
    }
}

/// Defines guarantees that Producer must follow delivering records to SPU.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum DeliverySemantic {
    /// Send records without waiting for the response. `Fire and forget` approach.
    AtMostOnce,
    /// Send records, wait for the response and retry if an error occurs. Retry parameters,
    /// such as delay, retry strategy, timeout, etc., can be configured in [`RetryPolicy`].
    AtLeastOnce(RetryPolicy),
}

/// Defines parameters of retries in [`DeliverySemantic::AtLeastOnce`] delivery semantic.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct RetryPolicy {
    /// Max amount of retries. If `0`, no retries will be performed.
    pub max_retries: usize,

    /// Initial delay before the first retry.
    pub initial_delay: Duration,

    /// The upper limit for delay for [`RetryStrategy::ExponentialBackoff`] and
    /// [`RetryStrategy::FibonacciBackoff`] retry strategies.
    pub max_delay: Duration,

    /// Max duration for all retries.
    pub timeout: Duration,

    /// Defines the strategy of delays distribution.
    pub strategy: RetryStrategy,
}

impl Default for DeliverySemantic {
    fn default() -> Self {
        Self::AtLeastOnce(RetryPolicy::default())
    }
}

impl Display for DeliverySemantic {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl FromStr for DeliverySemantic {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "at_most_once" | "at-most-once" | "AtMostOnce" | "atMostOnce" | "atmostonce" => Ok(DeliverySemantic::AtMostOnce),
            "at_least_once" | "at-least-once" | "AtLeastOnce" | "atLeastOnce" | "atleastonce" => Ok(DeliverySemantic::default()),
            _ => Err(format!("unrecognized delivery semantic: {s}. Supported: at_most_once (AtMostOnce), at_least_once (AtLeastOnce)")),
        }
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: DEFAULT_MAX_RETRIES,
            initial_delay: DEFAULT_INITIAL_DELAY,
            max_delay: DEFAULT_MAX_DELAY,
            timeout: DEFAULT_RETRIES_TIMEOUT,
            strategy: RetryStrategy::ExponentialBackoff,
        }
    }
}

/// Strategy of delays distribution.
#[derive(Default, Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum RetryStrategy {
    /// A retry strategy is driven by a fixed interval between retries.
    FixedDelay,
    /// A retry strategy is driven by exponential back-off.
    /// For example, if you have five retries with initial delay of 10ms, the sequence will be:
    /// `[10ms, 100ms, 1s, 200s, 200s]`
    #[default]
    ExponentialBackoff,
    /// A retry strategy is driven by the Fibonacci series of intervals between retries.
    /// For example, if you have five retries with initial delay of 10ms, the sequence will be
    /// `[10ms, 10ms, 20ms, 30ms, 50ms]`
    FibonacciBackoff,
}

#[derive(Debug)]
enum RetryPolicyIter {
    FixedDelay(FixedDelay),
    ExponentialBackoff(ExponentialBackoff),
    FibonacciBackoff(FibonacciBackoff),
}

impl Iterator for RetryPolicyIter {
    type Item = Duration;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            RetryPolicyIter::FixedDelay(iter) => iter.next(),
            RetryPolicyIter::ExponentialBackoff(iter) => iter.next(),
            RetryPolicyIter::FibonacciBackoff(iter) => iter.next(),
        }
    }
}

impl RetryPolicy {
    pub fn iter(&self) -> impl Iterator<Item = Duration> + Debug + Send {
        match self.strategy {
            RetryStrategy::FixedDelay => {
                RetryPolicyIter::FixedDelay(FixedDelay::new(self.initial_delay))
            }
            RetryStrategy::ExponentialBackoff => RetryPolicyIter::ExponentialBackoff(
                ExponentialBackoff::from_millis(self.initial_delay.as_millis() as u64)
                    .max_delay(self.max_delay),
            ),
            RetryStrategy::FibonacciBackoff => RetryPolicyIter::FibonacciBackoff(
                FibonacciBackoff::new(self.initial_delay).max_delay(self.max_delay),
            ),
        }
        .take(self.max_retries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_policy_fixed_iter() {
        //given
        let duration = Duration::from_millis(10);
        let policy = RetryPolicy {
            max_retries: 3,
            initial_delay: duration,
            strategy: RetryStrategy::FixedDelay,
            ..Default::default()
        };

        //when
        let iter = policy.iter();

        //then
        assert_eq!(iter.collect::<Vec<Duration>>(), [duration; 3])
    }

    #[test]
    fn test_retry_policy_exponential_iter() {
        //given
        let duration = Duration::from_millis(10);
        let max_duration = Duration::from_millis(2000);
        let policy = RetryPolicy {
            max_retries: 5,
            initial_delay: duration,
            max_delay: max_duration,
            strategy: RetryStrategy::ExponentialBackoff,
            ..Default::default()
        };

        //when
        let iter = policy.iter();

        //then
        assert_eq!(
            iter.collect::<Vec<Duration>>(),
            [
                duration,
                Duration::from_millis(100),
                Duration::from_millis(1000),
                max_duration,
                max_duration
            ]
        )
    }

    #[test]
    fn test_retry_policy_fibonacci_iter() {
        //given
        let duration = Duration::from_millis(10);
        let max_duration = Duration::from_millis(30);
        let policy = RetryPolicy {
            max_retries: 5,
            initial_delay: duration,
            max_delay: max_duration,
            strategy: RetryStrategy::FibonacciBackoff,
            ..Default::default()
        };

        //when
        let iter = policy.iter();

        //then
        assert_eq!(
            iter.collect::<Vec<Duration>>(),
            [
                duration,
                Duration::from_millis(10),
                Duration::from_millis(20),
                max_duration,
                max_duration
            ]
        )
    }

    #[test]
    fn test_retry_policy_never_retry() {
        //given
        let policy = RetryPolicy {
            max_retries: 0,
            ..Default::default()
        };

        //when
        let iter = policy.iter();

        //then
        assert_eq!(iter.collect::<Vec<Duration>>(), [])
    }
}

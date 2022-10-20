pub(crate) struct StorageMetrics {
    context: opentelemetry::Context,
    records: opentelemetry::metrics::Counter<u64>,
    bytes: opentelemetry::metrics::Counter<u64>,
}

impl StorageMetrics {
    pub(crate) fn new() -> Self {
        let context = opentelemetry::Context::new();
        let meter = opentelemetry::global::meter("storage");
        let records = meter.u64_counter("fluvio.storage.records").init();
        let bytes = meter.u64_counter("fluvio.storage.io").init();

        Self {
            context,
            records,
            bytes,
        }
    }

    pub(crate) fn with_topic<'a>(&'a self, topic: &'a str) -> StorageMetricsTopic {
        StorageMetricsTopic {
            metrics: self,
            topic,
        }
    }
}

pub(crate) struct StorageMetricsTopic<'a> {
    metrics: &'a StorageMetrics,
    topic: &'a str,
}

impl<'a> StorageMetricsTopic<'a> {
    pub(crate) fn with_partition(&'_ self, partition: i32) -> StorageMetricsTopicPartition {
        StorageMetricsTopicPartition {
            topic_metrics: self,
            partition,
        }
    }
}

pub(crate) struct StorageMetricsTopicPartition<'c> {
    topic_metrics: &'c StorageMetricsTopic<'c>,
    partition: i32,
}

impl<'c> StorageMetricsTopicPartition<'c> {
    pub(crate) fn add_records_read(&self, value: u64) {
        self.topic_metrics.metrics.records.add(
            &self.topic_metrics.metrics.context,
            value,
            &[
                opentelemetry::KeyValue::new("direction", "read"),
                opentelemetry::KeyValue::new("topic", self.topic_metrics.topic.to_owned()),
                opentelemetry::KeyValue::new("partition", self.partition as i64),
            ],
        );
    }

    pub(crate) fn add_bytes_read(&self, value: u64) {
        self.topic_metrics.metrics.bytes.add(
            &self.topic_metrics.metrics.context,
            value,
            &[
                opentelemetry::KeyValue::new("direction", "read"),
                opentelemetry::KeyValue::new("topic", self.topic_metrics.topic.to_owned()),
                opentelemetry::KeyValue::new("partition", self.partition as i64),
            ],
        );
    }

    pub(crate) fn add_records_written(&self, value: u64) {
        self.topic_metrics.metrics.records.add(
            &self.topic_metrics.metrics.context,
            value,
            &[
                opentelemetry::KeyValue::new("direction", "write"),
                opentelemetry::KeyValue::new("topic", self.topic_metrics.topic.to_owned()),
                opentelemetry::KeyValue::new("partition", self.partition as i64),
            ],
        );
    }

    pub(crate) fn add_bytes_written(&self, value: u64) {
        self.topic_metrics.metrics.bytes.add(
            &self.topic_metrics.metrics.context,
            value,
            &[
                opentelemetry::KeyValue::new("direction", "write"),
                opentelemetry::KeyValue::new("topic", self.topic_metrics.topic.to_owned()),
                opentelemetry::KeyValue::new("partition", self.partition as i64),
            ],
        );
    }
}

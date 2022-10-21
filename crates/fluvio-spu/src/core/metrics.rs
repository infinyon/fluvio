pub(crate) struct SpuMetrics {
    context: opentelemetry::Context,
    records: opentelemetry::metrics::Counter<u64>,
    bytes: opentelemetry::metrics::Counter<u64>,
}

impl SpuMetrics {
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

    pub(crate) fn with_topic_partition<'a>(
        &'a self,
        topic: &'a str,
        partition: i32,
    ) -> SpuMetricsTopicPartition {
        SpuMetricsTopicPartition {
            metrics: self,
            topic,
            partition,
        }
    }
}

impl std::fmt::Debug for SpuMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpuMetrics").finish()
    }
}

pub(crate) struct SpuMetricsTopicPartition<'a> {
    metrics: &'a SpuMetrics,
    topic: &'a str,
    partition: i32,
}

impl<'a> SpuMetricsTopicPartition<'a> {
    pub(crate) fn add_records_read(&self, value: u64) {
        self.metrics.records.add(
            &self.metrics.context,
            value,
            &[
                opentelemetry::KeyValue::new("direction", "read"),
                opentelemetry::KeyValue::new("topic", self.topic.to_owned()),
                opentelemetry::KeyValue::new("partition", self.partition as i64),
            ],
        );
    }

    pub(crate) fn add_bytes_read(&self, value: u64) {
        self.metrics.bytes.add(
            &self.metrics.context,
            value,
            &[
                opentelemetry::KeyValue::new("direction", "read"),
                opentelemetry::KeyValue::new("topic", self.topic.to_owned()),
                opentelemetry::KeyValue::new("partition", self.partition as i64),
            ],
        );
    }

    pub(crate) fn add_records_written(&self, value: u64) {
        self.metrics.records.add(
            &self.metrics.context,
            value,
            &[
                opentelemetry::KeyValue::new("direction", "write"),
                opentelemetry::KeyValue::new("topic", self.topic.to_owned()),
                opentelemetry::KeyValue::new("partition", self.partition as i64),
            ],
        );
    }

    pub(crate) fn add_bytes_written(&self, value: u64) {
        self.metrics.bytes.add(
            &self.metrics.context,
            value,
            &[
                opentelemetry::KeyValue::new("direction", "write"),
                opentelemetry::KeyValue::new("topic", self.topic.to_owned()),
                opentelemetry::KeyValue::new("partition", self.partition as i64),
            ],
        );
    }
}

#[cfg(feature = "otel-metrics")]
pub(crate) struct ChainMetrics {
    context: opentelemetry::Context,
    invocations: opentelemetry::metrics::Counter<u64>,
    records: opentelemetry::metrics::Counter<u64>,
    bytes: opentelemetry::metrics::Counter<u64>,
}

#[cfg(feature = "otel-metrics")]
impl ChainMetrics {
    pub(crate) fn new() -> Self {
        let meter = opentelemetry::global::meter("smartengine");
        let context = opentelemetry::Context::new();
        let invocations = meter
            .u64_counter("fluvio.smartengine.chain.invocations")
            .init();
        let records = meter.u64_counter("fluvio.smartengine.chain.records").init();
        let bytes = meter.u64_counter("fluvio.smartengine.chain.bytes").init();

        Self {
            context,
            invocations,
            records,
            bytes,
        }
    }

    pub(crate) fn inc_invocations(&self) {
        self.invocations.add(&self.context, 1u64, &[]);
    }

    pub(crate) fn add_records_out(&self, value: u64) {
        self.records.add(
            &self.context,
            value,
            &[opentelemetry::KeyValue::new("direction", "transmit")],
        );
    }

    pub(crate) fn add_bytes_in(&self, value: u64) {
        self.bytes.add(
            &self.context,
            value,
            &[opentelemetry::KeyValue::new("direction", "receive")],
        );
    }
}

#[cfg(not(feature = "otel-metrics"))]
pub(crate) struct ChainMetrics;

#[cfg(not(feature = "otel-metrics"))]
impl ChainMetrics {
    pub(crate) fn new() -> Self {
        Self
    }

    pub(crate) fn inc_invocations(&self) {}

    pub(crate) fn add_records_out(&self, _value: u64) {}

    pub(crate) fn add_bytes_in(&self, _value: u64) {}
}

#[cfg(feature = "otel-metrics")]
pub(crate) struct SmartModuleMetrics {
    context: opentelemetry::Context,
    invocations: opentelemetry::metrics::Counter<u64>,
    records: opentelemetry::metrics::Counter<u64>,
    bytes: opentelemetry::metrics::Counter<u64>,
}

#[cfg(feature = "otel-metrics")]
impl SmartModuleMetrics {
    pub(crate) fn new() -> Self {
        let meter = opentelemetry::global::meter("smartengine");
        let context = opentelemetry::Context::new();
        let invocations = meter
            .u64_counter("fluvio.smartengine.smartmodule.invocations")
            .init();
        let records = meter
            .u64_counter("fluvio.smartengine.smartmodule.records")
            .init();
        let bytes = meter
            .u64_counter("fluvio.smartengine.smartmodule.bytes")
            .init();

        Self {
            context,
            invocations,
            records,
            bytes,
        }
    }

    pub(crate) fn inc_invocations(&self) {
        self.invocations.add(&self.context, 1u64, &[]);
    }

    pub(crate) fn add_records_out(&self, value: u64) {
        self.records.add(
            &self.context,
            value,
            &[opentelemetry::KeyValue::new("direction", "transmit")],
        );
    }

    pub(crate) fn add_bytes_in(&self, value: u64) {
        self.bytes.add(
            &self.context,
            value,
            &[opentelemetry::KeyValue::new("direction", "receive")],
        );
    }
}

#[cfg(not(feature = "otel-metrics"))]
pub(crate) struct SmartModuleMetrics;

#[cfg(not(feature = "otel-metrics"))]
impl SmartModuleMetrics {
    pub(crate) fn new() -> Self {
        Self
    }

    pub(crate) fn inc_invocations(&self) {}

    pub(crate) fn add_records_out(&self, _value: u64) {}

    pub(crate) fn add_bytes_in(&self, _value: u64) {}
}

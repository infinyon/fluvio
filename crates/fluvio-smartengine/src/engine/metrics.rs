use std::cmp::max;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};

const DEFAULT_ORDERING: Ordering = Ordering::Relaxed;

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct SmartModuleChainMetrics {
    bytes_in: AtomicU64,
    records_out: AtomicU64,
    invocation_count: AtomicU64,
    fuel_used: AtomicU64,
    // CPU time in milliseconds
    // allow this to be missing for deserialization for legacy use
    #[serde(default)]
    cpu_ms: AtomicU64,
}

impl SmartModuleChainMetrics {
    pub fn add_bytes_in(&self, value: u64) {
        self.bytes_in.fetch_add(value, DEFAULT_ORDERING);
        self.invocation_count.fetch_add(1, DEFAULT_ORDERING);
    }

    pub fn add_records_out(&self, value: u64) {
        self.records_out.fetch_add(value, DEFAULT_ORDERING);
    }

    pub fn add_fuel_used(&self, fuel: u64, cpu_elapsed: Duration) {
        let cpu_ms = cpu_elapsed.as_millis() as u64;
        self.cpu_ms.fetch_add(max(cpu_ms, 1), DEFAULT_ORDERING);
        self.fuel_used.fetch_add(fuel, DEFAULT_ORDERING);
    }

    pub fn bytes_in(&self) -> u64 {
        self.bytes_in.load(DEFAULT_ORDERING)
    }

    pub fn records_out(&self) -> u64 {
        self.records_out.load(DEFAULT_ORDERING)
    }

    pub fn fuel_used(&self) -> u64 {
        self.fuel_used.load(DEFAULT_ORDERING)
    }

    pub fn invocation_count(&self) -> u64 {
        self.invocation_count.load(DEFAULT_ORDERING)
    }

    pub fn cpu_ms(&self) -> u64 {
        self.cpu_ms.load(DEFAULT_ORDERING)
    }
}

#[cfg(test)]
mod t_smartmodule_metrics {

    #[test]
    fn test_metrics() {
        use std::time::Duration;
        use super::SmartModuleChainMetrics;

        let metrics = SmartModuleChainMetrics::default();
        let elapsed = Duration::from_millis(100);
        let fuel = 100;
        metrics.add_fuel_used(fuel, elapsed);

        assert_eq!(metrics.cpu_ms(), 100);

        let _out = serde_json::to_string(&metrics).expect("serialize");
    }

    #[test]
    fn last_version() {
        use super::SmartModuleChainMetrics;

        // previous version, no cpu_ms
        let input = r#"{"bytes_in":0,"records_out":0,"invocation_count":0,"fuel_used":0}"#;
        let metrics: SmartModuleChainMetrics = serde_json::from_str(input).expect("deserialize");
        assert_eq!(metrics.cpu_ms(), 0);

        // check behavior w/ extra property
        let input =
            r#"{"bytes_in":0,"records_out":0,"invocation_count":0,"fuel_used":0, "extra": 1}"#;
        let metrics: SmartModuleChainMetrics =
            serde_json::from_str(input).expect("deserialize with extra");
        assert_eq!(metrics.cpu_ms(), 0);
    }
}

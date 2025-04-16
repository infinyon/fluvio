use std::cmp::max;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};

const DEFAULT_ORDERING: Ordering = Ordering::Relaxed;

#[derive(Serialize, Deserialize, Debug)]
pub struct SmartModuleChainMetrics {
    bytes_in: AtomicU64,
    records_out: AtomicU64,
    records_err: AtomicU64,
    invocation_count: AtomicU64,
    fuel_used: AtomicU64,
    // CPU time in milliseconds
    // allow this to be missing for deserialization for legacy use
    #[serde(default)]
    cpu_ms: AtomicU64,
    // Names of the SmartModules in the chain
    #[serde(default)]
    smartmodule_names: Vec<String>,
}

impl Clone for SmartModuleChainMetrics {
    fn clone(&self) -> Self {
        Self {
            bytes_in: AtomicU64::new(self.bytes_in.load(DEFAULT_ORDERING)),
            records_out: AtomicU64::new(self.records_out.load(DEFAULT_ORDERING)),
            records_err: AtomicU64::new(self.records_err.load(DEFAULT_ORDERING)),
            invocation_count: AtomicU64::new(self.invocation_count.load(DEFAULT_ORDERING)),
            fuel_used: AtomicU64::new(self.fuel_used.load(DEFAULT_ORDERING)),
            cpu_ms: AtomicU64::new(self.cpu_ms.load(DEFAULT_ORDERING)),
            smartmodule_names: self.smartmodule_names.clone(),
        }
    }
}

impl Default for SmartModuleChainMetrics {
    fn default() -> Self {
        Self::new(&[])
    }
}

impl SmartModuleChainMetrics {
    /// Create a new instance of SmartModuleChainMetrics
    /// with the given names of SmartModules.
    pub fn new(names: &[String]) -> Self {
        Self {
            bytes_in: AtomicU64::new(0),
            records_out: AtomicU64::new(0),
            records_err: AtomicU64::new(0),
            invocation_count: AtomicU64::new(0),
            fuel_used: AtomicU64::new(0),
            cpu_ms: AtomicU64::new(0),
            smartmodule_names: names.to_vec(),
        }
    }

    pub fn add_bytes_in(&self, value: u64) {
        self.bytes_in.fetch_add(value, DEFAULT_ORDERING);
    }

    pub fn add_invocation_count(&self, value: u64) {
        self.invocation_count.fetch_add(value, DEFAULT_ORDERING);
    }

    pub fn add_records_out(&self, value: u64) {
        self.records_out.fetch_add(value, DEFAULT_ORDERING);
    }

    pub fn add_records_err(&self, value: u64) {
        self.records_err.fetch_add(value, DEFAULT_ORDERING);
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

    pub fn records_err(&self) -> u64 {
        self.records_err.load(DEFAULT_ORDERING)
    }

    pub fn smartmodule_names(&self) -> &Vec<String> {
        &self.smartmodule_names
    }

    pub fn append(&self, other: &SmartModuleChainMetrics) {
        self.bytes_in
            .fetch_add(other.bytes_in.load(DEFAULT_ORDERING), DEFAULT_ORDERING);
        self.records_out
            .fetch_add(other.records_out.load(DEFAULT_ORDERING), DEFAULT_ORDERING);
        self.fuel_used
            .fetch_add(other.fuel_used.load(DEFAULT_ORDERING), DEFAULT_ORDERING);
        self.cpu_ms
            .fetch_add(other.cpu_ms.load(DEFAULT_ORDERING), DEFAULT_ORDERING);
        self.invocation_count.fetch_add(
            other.invocation_count.load(DEFAULT_ORDERING),
            DEFAULT_ORDERING,
        );
        self.records_err
            .fetch_add(other.records_err.load(DEFAULT_ORDERING), DEFAULT_ORDERING);
    }
    pub fn reset(&self) {
        self.bytes_in.store(0, DEFAULT_ORDERING);
        self.records_out.store(0, DEFAULT_ORDERING);
        self.fuel_used.store(0, DEFAULT_ORDERING);
        self.cpu_ms.store(0, DEFAULT_ORDERING);
        self.invocation_count.store(0, DEFAULT_ORDERING);
        self.records_err.store(0, DEFAULT_ORDERING);
    }
}

#[cfg(test)]
mod t_smartmodule_metrics {

    #[test]
    fn test_metrics() {
        use std::time::Duration;
        use super::SmartModuleChainMetrics;

        let sm_names = vec!["module1".to_string(), "module2".to_string()];
        let metrics = SmartModuleChainMetrics::new(&sm_names);
        let elapsed = Duration::from_millis(100);
        let fuel = 100;
        metrics.add_fuel_used(fuel, elapsed);

        assert_eq!(metrics.cpu_ms(), 100);
        assert_eq!(
            metrics.smartmodule_names(),
            &vec!["module1".to_string(), "module2".to_string()]
        );

        let out = serde_json::to_string(&metrics).expect("serialize");
        println!("metrics2: {:?}", out);
    }

    #[test]
    fn last_version() {
        use super::SmartModuleChainMetrics;

        // previous version, no cpu_ms
        let input =
            r#"{"bytes_in":0,"records_out":0,"invocation_count":0,"fuel_used":0,"records_err":0}"#;
        let metrics: SmartModuleChainMetrics = serde_json::from_str(input).expect("deserialize");
        assert_eq!(metrics.cpu_ms(), 0);

        // check behavior w/ extra property
        let input = r#"{"bytes_in":0,"records_out":0,"invocation_count":0,"fuel_used":0, "extra": 1, "records_err": 0}"#;
        let metrics: SmartModuleChainMetrics =
            serde_json::from_str(input).expect("deserialize with extra");
        assert_eq!(metrics.cpu_ms(), 0);
    }
}

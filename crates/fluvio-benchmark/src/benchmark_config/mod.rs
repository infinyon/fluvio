use std::time::Duration;

use serde::{Deserialize, Serialize};

pub mod benchmark_matrix;

pub mod benchmark_config;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Millis(u64);
impl Millis {
    pub fn new(millis: u64) -> Self {
        Self(millis)
    }
}

impl From<Millis> for Duration {
    fn from(m: Millis) -> Self {
        Duration::from_millis(m.0)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Seconds(u64);
impl Seconds {
    pub fn new(seconds: u64) -> Self {
        Self(seconds)
    }
}

impl From<Seconds> for Duration {
    fn from(m: Seconds) -> Self {
        Duration::from_secs(m.0)
    }
}

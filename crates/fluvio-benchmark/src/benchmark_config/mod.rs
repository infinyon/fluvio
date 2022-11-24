use std::time::Duration;

use serde::{Deserialize, Serialize};

pub mod benchmark_matrix;

pub mod benchmark_config;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Millis(u64);

impl From<Millis> for Duration {
    fn from(m: Millis) -> Self {
        Duration::from_millis(m.0)
    }
}

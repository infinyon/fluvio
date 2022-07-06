use std::ops::{Add, AddAssign};

use super::ClientStatsMetricRaw;

/// Update builder for `ClientStats`
/// A buffer for `ClientStats::update() for updating multiple metrics
#[derive(Debug, Default, Clone)]
pub struct ClientStatsUpdateBuilder {
    pub data: Vec<ClientStatsMetricRaw>,
}

/// Make it easy to combine multiple updates
impl Add for ClientStatsUpdateBuilder {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        let mut add = Vec::new();

        add.extend(&self.data);
        add.extend(other.data);

        Self { data: add }
    }
}

impl AddAssign for ClientStatsUpdateBuilder {
    fn add_assign(&mut self, other: Self) {
        self.data.extend(other.data);
    }
}

impl ClientStatsUpdateBuilder {
    /// Create a new `ClientStatsUpdateBuilder`
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a data sample to update
    pub fn push(&mut self, sample: ClientStatsMetricRaw) {
        self.data.push(sample);
    }

    /// Most recent data sample
    pub fn pop(&mut self) -> Option<ClientStatsMetricRaw> {
        self.data.pop()
    }
}

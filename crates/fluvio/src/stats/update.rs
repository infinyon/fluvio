use std::ops::{Add, AddAssign};

/// Update builder for `ClientStats` to allow
/// for update staging for multiple values to apply
#[derive(Debug, Default, Clone, Copy)]
pub struct ClientStatsUpdate {
    /// Data volume transferred, in bytes
    bytes: Option<u64>,
    /// CPU usage percentage
    cpu: Option<u32>,
    /// Latency of last transfer, in nanoseconds
    latency: Option<u64>,
    /// Memory usage of process, in kilobytes
    mem: Option<u64>,
    /// Number of records transferred
    records: Option<u64>,
    /// Last offset
    offset: Option<i32>,
}

/// Make it easy to combine multiple updates
/// `bytes`, `records`, and `latency` add their respective values together
/// `cpu`, `memory`, and `offset` replace their values w/ the `ClientStatsUpdate` on the Rhs
impl Add for ClientStatsUpdate {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        // These values should accumulate
        let bytes = match (self.bytes, other.bytes) {
            (Some(a), Some(b)) => Some(a + b),
            (opt_a, opt_b) => opt_a.or(opt_b),
        };

        let records = match (self.records, other.records) {
            (Some(a), Some(b)) => Some(a + b),
            (opt_a, opt_b) => opt_a.or(opt_b),
        };

        let latency = match (self.latency, other.latency) {
            (Some(a), Some(b)) => Some(a + b),
            (opt_a, opt_b) => opt_a.or(opt_b),
        };

        // But don't add these status values together.
        // Replace with `other` value if it exists
        let cpu = match (self.cpu, other.cpu) {
            (Some(_), Some(b)) => Some(b),
            (opt_a, opt_b) => opt_a.or(opt_b),
        };
        let mem = match (self.mem, other.mem) {
            (Some(_), Some(b)) => Some(b),
            (opt_a, opt_b) => opt_a.or(opt_b),
        };
        let offset = match (self.offset, other.offset) {
            (Some(_), Some(b)) => Some(b),
            (opt_a, opt_b) => opt_a.or(opt_b),
        };

        Self {
            bytes,
            cpu,
            latency,
            mem,
            records,
            offset,
        }
    }
}

impl AddAssign for ClientStatsUpdate {
    fn add_assign(&mut self, other: Self) {
        *self = *self + other
    }
}

impl ClientStatsUpdate {
    /// Create a new `ClientStatsUpdate`
    pub fn new() -> Self {
        Self::default()
    }

    /// Return the `latency` value in nanoseconds
    pub fn latency(&self) -> Option<u64> {
        self.latency
    }

    /// Set the `latency` value in nanoseconds
    pub fn set_latency(&mut self, latency: Option<u64>) -> Self {
        self.latency = latency;
        *self
    }

    /// Return the `bytes` value
    pub fn bytes(&self) -> Option<u64> {
        self.bytes
    }

    /// Set the `bytes` value
    pub fn set_bytes(&mut self, bytes: Option<u64>) -> Self {
        self.bytes = bytes;
        *self
    }

    /// Return the `records` value
    pub fn records(&self) -> Option<u64> {
        self.records
    }

    /// Set the `records` value
    pub fn set_records(&mut self, records: Option<u64>) -> Self {
        self.records = records;
        *self
    }

    /// Return the `mem` value
    pub fn mem(&self) -> Option<u64> {
        self.mem
    }

    /// Set the `mem` value
    pub fn set_mem(&mut self, mem: Option<u64>) -> Self {
        self.mem = mem;
        *self
    }

    /// Return the `cpu` value
    pub fn cpu(&self) -> Option<u32> {
        self.cpu
    }

    /// Set the `cpu` value
    /// The value expected is integer shifted percentage + 3 decimal places
    pub fn set_cpu(&mut self, cpu: Option<u32>) -> Self {
        //println!("Setting cpu for update: {:#?}", cpu);
        self.cpu = cpu;
        *self
    }

    /// Return the `offset` value`
    pub fn offset(&self) -> Option<i32> {
        self.offset
    }

    /// Set the `offset` value
    pub fn set_offset(&mut self, offset: Option<i32>) -> Self {
        //println!("Setting offset for update: {:#?}", offset);
        self.offset = offset;
        *self
    }
}

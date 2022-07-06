use std::ops::{Add, AddAssign};
/// Update builder for `ClientStats`
#[derive(Debug, Default, Clone, Copy)]
pub struct ClientStatsUpdate {
    bytes: Option<u64>,
    cpu: Option<u32>,
    latency: Option<u64>,
    mem: Option<u64>,
    records: Option<u64>,
    offset: Option<i32>,
}

impl Add for ClientStatsUpdate {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        let bytes = match (self.bytes, other.bytes) {
            (Some(a), Some(b)) => Some(a + b),
            (opt_a, opt_b) => opt_a.or(opt_b),
        };
        let cpu = match (self.cpu, other.cpu) {
            (Some(a), Some(b)) => Some(a + b),
            (opt_a, opt_b) => opt_a.or(opt_b),
        };
        let latency = match (self.latency, other.latency) {
            (Some(a), Some(b)) => Some(a + b),
            (opt_a, opt_b) => opt_a.or(opt_b),
        };
        let mem = match (self.mem, other.mem) {
            (Some(a), Some(b)) => Some(a + b),
            (opt_a, opt_b) => opt_a.or(opt_b),
        };
        let records = match (self.records, other.records) {
            (Some(a), Some(b)) => Some(a + b),
            (opt_a, opt_b) => opt_a.or(opt_b),
        };
        //let offset = match (self.offset, other.offset) {
        //    (Some(a), Some(b)) => Some(a + b),
        //    (opt_a, opt_b) => opt_a.or(opt_b),
        //};

        Self {
            bytes,
            cpu,
            latency,
            mem,
            records,
            offset: self.offset,
        }
    }
}

impl AddAssign for ClientStatsUpdate {
    fn add_assign(&mut self, other: Self) {
        *self = *self + other
    }
}

impl ClientStatsUpdate {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn latency(&self) -> Option<u64> {
        self.latency
    }
    pub fn set_latency(&mut self, latency: Option<u64>) -> Self {
        //println!("Setting latency for update: {latency:#?}",);
        self.latency = latency;
        *self
    }

    pub fn bytes(&self) -> Option<u64> {
        self.bytes
    }

    pub fn set_bytes(&mut self, bytes: Option<u64>) -> Self {
        //println!("Setting bytes for update: {:#?}", bytes);
        self.bytes = bytes;
        *self
    }

    pub fn records(&self) -> Option<u64> {
        self.records
    }

    pub fn set_records(&mut self, records: Option<u64>) -> Self {
        //println!("Setting records for update: {:#?}", records);
        self.records = records;
        *self
    }

    pub fn mem(&self) -> Option<u64> {
        self.mem
    }

    pub fn set_mem(&mut self, mem: Option<u64>) -> Self {
        //println!("Setting mem for update: {:#?}", mem);
        self.mem = mem;
        *self
    }

    pub fn cpu(&self) -> Option<u32> {
        self.cpu
    }

    // The value stored is percentage + 3 decimal places
    // Divide by 100.0 to get CPU utilization
    pub fn set_cpu(&mut self, cpu: Option<u32>) -> Self {
        //println!("Setting cpu for update: {:#?}", cpu);
        self.cpu = cpu;
        *self
    }

    pub fn offset(&self) -> Option<i32> {
        self.offset
    }

    pub fn set_offset(&mut self, offset: Option<i32>) -> Self {
        //println!("Setting offset for update: {:#?}", offset);
        self.offset = offset;
        *self
    }
}

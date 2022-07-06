use std::{
    time::{Duration, Instant},
};
use chrono::{DateTime, Utc};
use std::sync::Arc;
use async_lock::RwLock;
use sysinfo::{self, PidExt};

#[derive(Debug, Clone)]
pub struct ClientStats {
    start_time: Instant,
    pid: u64,
    last_updated: DateTime<Utc>,
    last_latency: Duration,
    last_bytes: usize, //ByteSize
    num_records: u64,
    total_bytes: usize,
    last_mem: u64,
    last_cpu: f32,
}

impl Default for ClientStats {
    fn default() -> Self {
        // Get pid
        let pid = if let Ok(pid) = sysinfo::get_current_pid() {
            pid.as_u32()
        } else {
            0
        };

        ClientStats {
            start_time: Instant::now(),
            pid: pid.into(),
            last_updated: Utc::now(),
            last_latency: Duration::ZERO,
            last_bytes: 0,
            num_records: 0,
            total_bytes: 0,
            last_mem: 0,
            last_cpu: 0.0,
        }
    }
}

impl ClientStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_shared() -> Arc<RwLock<Self>> {
        Arc::new(RwLock::new(Self::new()))
    }

    pub fn update(&mut self, update: ClientStatsUpdate) {
        if let Some(bytes) = update.bytes {
            self.last_bytes = bytes;
            self.total_bytes += bytes;
        }

        if let Some(cpu) = update.cpu {
            self.last_cpu = cpu;
        }

        if let Some(latency) = update.latency {
            self.last_latency = latency;
        }

        if let Some(mem) = update.mem {
            self.last_mem = mem;
        }

        if let Some(records) = update.records {
            self.num_records += records;
        }

        self.last_updated = Utc::now();
    }

    pub fn snapshot(&self) -> ClientStats {
        self.clone().to_owned()
    }
}

#[derive(Debug, Clone, Default)]
pub struct ClientStatsUpdate {
    bytes: Option<usize>,
    cpu: Option<f32>,
    latency: Option<Duration>,
    mem: Option<u64>,
    records: Option<u64>,
}

impl ClientStatsUpdate {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn latency(&self, latency: Option<Duration>) -> Self {
        let mut new = self.clone();
        new.latency = latency;
        new
    }

    pub fn bytes(&self, bytes: Option<usize>) -> Self {
        let mut new = self.clone();
        new.bytes = bytes;
        new
    }

    pub fn records(&self, records: Option<u64>) -> Self {
        let mut new = self.clone();
        new.records = records;
        new
    }

    pub fn mem(&self, mem: Option<u64>) -> Self {
        let mut new = self.clone();
        new.mem = mem;
        new
    }

    pub fn cpu(&self, cpu: Option<f32>) -> Self {
        let mut new = self.clone();
        new.cpu = cpu;
        new
    }
}

//impl DerefMut for Arc<ClientStats> {
//    fn deref_mut(&mut self) -> &mut Self::Target {
//        &mut self
//    }
//}

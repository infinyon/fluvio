use super::{ClientStats, ClientStatsDataCollect, unix_timestamp_nanos};

use chrono::{NaiveDateTime, DateTime, Utc};

use quantities::{prelude::*, AMNT_ONE};
use quantities::datathroughput::{
    DataThroughput, TERABYTE_PER_SECOND, GIGABYTE_PER_SECOND, MEGABYTE_PER_SECOND,
    KILOBYTE_PER_SECOND,
};
use quantities::datavolume::{DataVolume, BYTE, KILOBYTE, MEGABYTE, GIGABYTE, TERABYTE};
use quantities::duration::{
    Duration as QuantDuration, MINUTE, SECOND, MILLISECOND, MICROSECOND, NANOSECOND,
};
// This struct is heavily utilizing
// `quantities` crate for managing human readable unit conversions
#[derive(Debug, Clone, Copy)]
pub struct ClientStatsDataPoint {
    start_time: DateTime<Utc>,
    pid: u32,
    offset: i32,
    last_updated: DateTime<Utc>,
    last_latency: QuantDuration,
    last_bytes: DataVolume,
    num_records: u64,
    total_bytes: DataVolume,
    last_mem: DataVolume,
    last_cpu: f32,
    stats_collect: ClientStatsDataCollect,
}

impl From<&ClientStats> for ClientStatsDataPoint {
    fn from(current: &ClientStats) -> Self {
        Self {
            start_time: {
                let timestamp = current.start_time();
                let naive = NaiveDateTime::from_timestamp(timestamp, 0);
                DateTime::from_utc(naive, Utc)
            },
            pid: current.pid(),
            offset: current.offset(),
            last_updated: {
                let timestamp = current.last_updated();
                let naive = NaiveDateTime::from_timestamp(timestamp, 0);
                DateTime::from_utc(naive, Utc)
            },
            last_latency: {
                let scalar: AmountT = (current.last_latency() as u32).into();
                Self::convert_to_largest_time_unit(scalar * NANOSECOND)
            },
            last_bytes: {
                let scalar: AmountT = (current.last_bytes() as u32).into();
                Self::convert_to_largest_data_unit(scalar * BYTE)
            },
            num_records: current.records(),
            total_bytes: {
                let scalar: AmountT = (current.total_bytes() as u32).into();
                Self::convert_to_largest_data_unit(scalar * BYTE)
            },
            last_mem: {
                let scalar: AmountT = (current.memory() as u32).into();
                Self::convert_to_largest_data_unit(scalar * BYTE)
            },
            last_cpu: { (current.cpu() as f32) / 1_000.0 }, // We have 3 decimal places
            stats_collect: current.stats_collect,
        }
    }
}

impl ClientStatsDataPoint {
    /// Return the start time
    pub fn start_time(&self) -> DateTime<Utc> {
        self.start_time
    }

    /// Return the last updated time
    pub fn last_updated(&self) -> DateTime<Utc> {
        self.last_updated
    }

    /// Return configured collection option
    pub fn stats_collect(&self) -> ClientStatsDataCollect {
        self.stats_collect
    }

    /// Return the throughput of the last batch transfer
    pub fn throughput(&self) -> DataThroughput {
        Self::covert_to_largest_throughput_unit(self.last_bytes() / self.last_latency())
    }

    /// Return the throughput of the session
    pub fn total_throughput(&self) -> DataThroughput {
        Self::covert_to_largest_throughput_unit(self.total_bytes() / self.uptime())
    }

    /// Return the pid of the client
    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// Returns the offset last seen
    pub fn offset(&self) -> i32 {
        self.offset
    }

    /// Returns the last data transfer size in bytes
    pub fn last_bytes(&self) -> DataVolume {
        self.last_bytes
    }

    /// Returns the accumulated data transfer size in bytes
    pub fn total_bytes(&self) -> DataVolume {
        self.total_bytes
    }

    /// Returns the latency of last transfer -> Duration {
    pub fn last_latency(&self) -> QuantDuration {
        self.last_latency
    }

    /// Returns the last memory usage sample
    pub fn memory(&self) -> DataVolume {
        self.last_mem
    }

    /// Returns the last cpu usage sample as a percentage (%)
    pub fn cpu(&self) -> f32 {
        self.last_cpu
    }

    /// Returns the current uptime of the client
    pub fn uptime(&self) -> QuantDuration {
        let start = self.start_time.timestamp_nanos();
        let now = unix_timestamp_nanos();

        let scalar: AmountT = ((now - start) as u32).into();

        Self::convert_to_largest_time_unit(scalar * NANOSECOND)
    }

    /// Returns the number of records transferred
    pub fn records(&self) -> u64 {
        self.num_records
    }

    fn convert_to_largest_time_unit(ref_value: QuantDuration) -> QuantDuration {
        let convert_unit = if ref_value > (AMNT_ONE * MINUTE) {
            Some(MINUTE)
        } else if ref_value > (AMNT_ONE * SECOND) {
            Some(SECOND)
        } else if ref_value > (AMNT_ONE * MILLISECOND) {
            Some(MILLISECOND)
        } else if ref_value > (AMNT_ONE * MICROSECOND) {
            Some(MICROSECOND)
        } else if ref_value > (AMNT_ONE * NANOSECOND) {
            Some(NANOSECOND)
        } else {
            None
        };

        if let Some(bigger_unit) = convert_unit {
            ref_value.convert(bigger_unit)
        } else {
            ref_value
        }
    }

    fn convert_to_largest_data_unit(ref_value: DataVolume) -> DataVolume {
        let convert_unit = if ref_value > (AMNT_ONE * TERABYTE) {
            Some(TERABYTE)
        } else if ref_value > (AMNT_ONE * GIGABYTE) {
            Some(GIGABYTE)
        } else if ref_value > (AMNT_ONE * MEGABYTE) {
            Some(MEGABYTE)
        } else if ref_value > (AMNT_ONE * KILOBYTE) {
            Some(KILOBYTE)
        } else if ref_value > (AMNT_ONE * BYTE) {
            Some(BYTE)
        } else {
            None
        };

        if let Some(bigger_unit) = convert_unit {
            ref_value.convert(bigger_unit)
        } else {
            ref_value
        }
    }

    fn covert_to_largest_throughput_unit(ref_value: DataThroughput) -> DataThroughput {
        let convert_unit = if ref_value > (AMNT_ONE * TERABYTE_PER_SECOND) {
            Some(TERABYTE_PER_SECOND)
        } else if ref_value > (AMNT_ONE * GIGABYTE_PER_SECOND) {
            Some(GIGABYTE_PER_SECOND)
        } else if ref_value > (AMNT_ONE * MEGABYTE_PER_SECOND) {
            Some(MEGABYTE_PER_SECOND)
        } else if ref_value > (AMNT_ONE * KILOBYTE_PER_SECOND) {
            Some(KILOBYTE_PER_SECOND)
        } else {
            None
        };

        if let Some(bigger_unit) = convert_unit {
            ref_value.convert(bigger_unit)
        } else {
            ref_value
        }
    }
}

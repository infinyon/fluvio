use crate::FluvioError;
use super::{ClientStats, ClientStatsDataCollect, unix_timestamp_nanos};

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
    /// Start time when corresponding `ClientStats` struct was created
    /// This is Unix Epoch time, in nanoseconds
    start_time: i64,
    /// PID of the client process
    pid: u32,
    /// Offset from last batch seen
    offset: i32,
    /// Last time corresponding `ClientStats` struct values were updated
    /// This is Unix Epoch time, in nanoseconds
    last_updated: i64,
    /// Time it took to complete last data transfer, in nanoseconds
    last_latency: QuantDuration,
    /// Data volume of last data transfer
    last_bytes: DataVolume,
    /// Total number of records processed since struct created
    num_records: u64,
    /// Data volume of all data transferred
    total_bytes: DataVolume,
    /// Last polled memory usage of client process
    last_mem: DataVolume,
    /// Last polled CPU usage in percent (%)
    /// Adjusted for host system's # of CPU cores
    last_cpu: f32,
    /// Configuration of what data client is to collect
    stats_collect: ClientStatsDataCollect,
}

/// This conversion from `ClientStats` collects the corresponding field values
/// and converts the values into `quantities` types. Supporting unit conversions
/// for representing data values and units for human consumption
impl From<&ClientStats> for ClientStatsDataPoint {
    fn from(current: &ClientStats) -> Self {
        Self {
            start_time: { current.start_time() },
            pid: current.pid(),
            offset: current.offset(),
            last_updated: { current.last_updated() },
            last_latency: {
                #[cfg(not(target_arch = "wasm32"))]
                let scalar: AmountT = (current.last_latency() as u32).into();
                #[cfg(target_arch = "wasm32")]
                let scalar: AmountT = (current.last_latency() as u16).into();
                Self::convert_to_largest_time_unit(scalar * NANOSECOND)
            },
            last_bytes: {
                #[cfg(not(target_arch = "wasm32"))]
                let scalar: AmountT = (current.last_bytes() as u32).into();
                #[cfg(target_arch = "wasm32")]
                let scalar: AmountT = (current.last_bytes() as u16).into();
                Self::convert_to_largest_data_unit(scalar * BYTE)
            },
            num_records: current.records(),
            total_bytes: {
                #[cfg(not(target_arch = "wasm32"))]
                let scalar: AmountT = (current.total_bytes() as u32).into();
                #[cfg(target_arch = "wasm32")]
                let scalar: AmountT = (current.total_bytes() as u16).into();
                Self::convert_to_largest_data_unit(scalar * BYTE)
            },
            last_mem: {
                #[cfg(not(target_arch = "wasm32"))]
                let scalar: AmountT = (current.memory() as u32).into();
                #[cfg(target_arch = "wasm32")]
                let scalar: AmountT = (current.memory() as u16).into();
                Self::convert_to_largest_data_unit(scalar * KILOBYTE)
            },
            last_cpu: { (current.cpu() as f32) / 1_000.0 }, // We have 3 decimal places
            stats_collect: current.stats_collect,
        }
    }
}

impl ClientStatsDataPoint {
    /// Return the start time in Unix Epoch, in nanoseconds
    pub fn start_time(&self) -> i64 {
        self.start_time
    }

    /// Return the last updated time in Unix Epoch, in nanoseconds
    pub fn last_updated(&self) -> i64 {
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
        let time_elapsed = unix_timestamp_nanos() - self.start_time;
        let scalar: AmountT = (time_elapsed as f32).into();
        Self::convert_to_largest_time_unit(scalar * NANOSECOND)
    }

    /// Returns the number of records transferred
    pub fn records(&self) -> u64 {
        self.num_records
    }

    /// Generates a CSV header string. Columns returned based on client's configured `ClientStatsDataCollect`
    pub fn csv_header(&self) -> Result<String, FluvioError> {
        if self.stats_collect() == ClientStatsDataCollect::None {
            return Err(FluvioError::Other(
                "Client is not collecting stats".to_string(),
            ));
        }

        let mut header = vec![
            "pid".to_string(),
            "start_time_ns".to_string(),
            "last_updated_ns".to_string(),
        ];

        let data_related = vec![
            "offset".to_string(),
            "total_records".to_string(),
            "total_bytes".to_string(),
            "last_latency_ns".to_string(),
            "last_bytes".to_string(),
            "throughput_kbps".to_string(),
        ];

        let system_related = vec!["last_mem_kb".to_string(), "last_cpu_pct".to_string()];

        if (self.stats_collect() == ClientStatsDataCollect::All)
            || (self.stats_collect() == ClientStatsDataCollect::Data)
        {
            header.extend(data_related);
        }

        if (self.stats_collect() == ClientStatsDataCollect::All)
            || (self.stats_collect() == ClientStatsDataCollect::System)
        {
            header.extend(system_related);
        }

        // Render header as CSV w/ trailing newline
        header.push("\n".to_string());
        Ok(header.join(","))
    }

    /// Generates a CSV row. Columns returned based on client's configured `ClientStatsDataCollect`
    pub fn csv_datapoint(&self) -> Result<String, FluvioError> {
        if self.stats_collect() == ClientStatsDataCollect::None {
            return Err(FluvioError::Other(
                "Client is not collecting stats".to_string(),
            ));
        }

        let mut data_point = vec![
            self.pid().to_string(),
            self.start_time().to_string(),
            self.last_updated().to_string(),
        ];

        let data_related = vec![
            self.offset().to_string(),
            self.records().to_string(),
            (self.total_bytes().equiv_amount(BYTE) as u64).to_string(),
            self.last_latency().equiv_amount(NANOSECOND).to_string(),
            (self.last_bytes().equiv_amount(BYTE) as u64).to_string(),
            format!("{:.2}", self.throughput().equiv_amount(KILOBYTE_PER_SECOND)),
        ];

        let system_related = vec![
            format!("{:.2}", self.memory().equiv_amount(KILOBYTE)),
            self.cpu().to_string(),
        ];

        if (self.stats_collect() == ClientStatsDataCollect::All)
            || (self.stats_collect() == ClientStatsDataCollect::Data)
        {
            data_point.extend(data_related);
        }

        if (self.stats_collect() == ClientStatsDataCollect::All)
            || (self.stats_collect() == ClientStatsDataCollect::System)
        {
            data_point.extend(system_related);
        }

        // Render header as CSV w/ trailing newline
        data_point.push("\n".to_string());
        Ok(data_point.join(","))
    }

    /// Convert given time quantity into largest divisible unit type of `second`
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

    /// Convert given data volume into largest divisible unit type of `byte`
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

    /// Convert given data throughput into largest divisible unit type of `byte per second`
    /// Rate will always be with respect to unit per second
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

use serde::{Serialize, Deserialize};
use fluvio_smartmodule::{smartmodule, Result, Record, RecordData};

#[derive(Default, Serialize, Deserialize)]
struct IncrementalAverage {
    average: f64,
    count: u32,
}

impl IncrementalAverage {
    /// Implement the formula for calculating an incremental average.
    ///
    /// https://math.stackexchange.com/questions/106700/incremental-averageing
    fn add_value(&mut self, value: f64) {
        self.count += 1;
        let new_count_float = f64::from(self.count);
        let value_average_difference = value - self.average;
        let difference_over_count = value_average_difference / new_count_float;
        let new_average = self.average + difference_over_count;
        self.average = new_average;
    }
}

#[smartmodule(aggregate)]
pub fn aggregate(accumulator: RecordData, current: &Record) -> Result<RecordData> {
    // Parse the average from JSON
    let mut average: IncrementalAverage =
        serde_json::from_slice(accumulator.as_ref()).unwrap_or_default();

    // Parse the new value as a 64-bit float
    let value = std::str::from_utf8(current.value.as_ref())?
        .trim()
        .parse::<f64>()?;
    average.add_value(value);

    let output = serde_json::to_vec_pretty(&average)?;
    Ok(output.into())
}

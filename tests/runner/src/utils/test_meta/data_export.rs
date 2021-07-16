use super::chart_builder::{FluvioTimeData, FluvioPercentileData};
use std::fs::File;
use hdrhistogram::Histogram;

pub struct DataExporter;

impl DataExporter {
    pub fn timeseries_as_csv(data: Vec<FluvioTimeData>, save_path: &str) {
        let file = File::create(save_path).expect("Couldn't create file");

        let mut wtr = csv::Writer::from_writer(file);

        for d in data {
            wtr.serialize(d)
                .expect("Unable to serialize datapoint to csv");
        }
        wtr.flush().expect("Unable to write file");
    }

    // We want to take the histogram, and then map over value at percentile from 0.0-100.0
    // percentile chart
    pub fn percentile_as_csv(data: Histogram<u64>, save_path: &str) {
        let file = File::create(save_path).expect("Couldn't create file");

        let mut wtr = csv::Writer::from_writer(file);

        for p in 0..101 {
            for d in 0..10 {
                let percentile_str = format!("{}.{}", p, d);
                let percentile: f32 = percentile_str
                    .parse::<f32>()
                    .expect("Could not parse percentile as f32");

                let hist_data = data.value_at_percentile(percentile as f64);
                //println!("{}%: {:?}", percentile, hist_data);

                // Serialize to csv
                wtr.serialize(FluvioPercentileData {
                    percentile,
                    data: hist_data as f32,
                })
                .expect("Unable to serialize to csv");

                if (p == 100) && (d == 0) {
                    break;
                }
            }
        }

        wtr.flush().expect("Unable to write file");
    }
}

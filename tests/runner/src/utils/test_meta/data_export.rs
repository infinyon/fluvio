use super::chart_builder::FluvioTimeData;
use std::fs::File;

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
}

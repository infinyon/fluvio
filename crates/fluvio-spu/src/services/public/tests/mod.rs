use std::sync::Arc;

use fluvio_protocol::{
    fixture::BatchProducer,
    record::{RecordData, Record, RecordSet},
};

mod stream_fetch;
mod produce;

/// create records that can be filtered
fn create_filter_records(records: u16) -> RecordSet {
    BatchProducer::builder()
        .records(records)
        .record_generator(Arc::new(generate_record))
        .build()
        .expect("batch")
        .records()
}

fn generate_record(record_index: usize, _producer: &BatchProducer) -> Record {
    let msg = match record_index {
        0 => "b".repeat(100),
        1 => "a".repeat(100),
        _ => "z".repeat(100),
    };

    Record::new(RecordData::from(msg))
}

use std::error::Error;
use std::io::Cursor;

use fluvio_spu_schema::produce::{DefaultProduceRequest};
use fluvio_protocol::Decoder;
use fluvio_spu_schema::Isolation;

// Test Specification:
//
// A request was encoded and written to a file, using the following code:
//
// ```rust
//     let mut batch = Batch::new();
//     batch.add_record(Record::default());
//     let records: RecordSet<RawRecords> = RecordSet {
//         batches: vec![batch.try_into()?],
//     };
//     let partition_request = DefaultPartitionRequest {
//         partition_index: 1,
//         records,
//     };
//     let topic_request = DefaultTopicRequest {
//         name: "topic".to_string(),
//         partitions: vec![partition_request],
//         data: Default::default(),
//     };
//     let request: DefaultProduceRequest = ProduceRequest {
//         transactional_id: Some("transaction_id".to_owned()),
//         acks: -1,
//         timeout_ms: 1000,
//         topics: vec![topic_request],
//         data: Default::default(),
//     };
//     let bytes = request.as_bytes(7)?;
// ```
#[test]
fn test_binary_compatibility() -> Result<(), Box<dyn Error>> {
    let old_encoded = std::fs::read("./tests/produce_request.bin")?;
    let decoded: DefaultProduceRequest =
        Decoder::decode_from(&mut Cursor::new(old_encoded), 7).expect("decoded");

    assert_eq!(decoded.timeout.as_millis(), 1000);
    assert_eq!(decoded.transactional_id, Some("transaction_id".to_owned()));
    assert_eq!(decoded.isolation, Isolation::ReadCommitted);

    assert_eq!(decoded.topics[0].name, "topic".to_owned());
    assert_eq!(decoded.topics[0].partitions[0].partition_index, 1);
    assert_eq!(decoded.topics[0].partitions[0].records.batches.len(), 1);
    Ok(())
}

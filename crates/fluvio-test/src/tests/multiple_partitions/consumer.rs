use std::collections::HashSet;
use fluvio_test_util::test_meta::environment::EnvDetail;
use fluvio_test_util::test_runner::test_driver::TestDriver;
use futures_lite::StreamExt;
use fluvio::Offset;
use tracing::{instrument, Instrument, debug_span};

use super::MultiplePartitionTestCase;

#[instrument(skip(test_driver))]
pub async fn consumer_stream(test_driver: &TestDriver, option: MultiplePartitionTestCase) {
    let consumer = test_driver
        .get_all_partitions_consumer(&option.environment.base_topic_name())
        .instrument(debug_span!("multipartition_consumer_create"))
        .await;
    let mut stream = consumer
        .stream(Offset::beginning())
        .instrument(debug_span!("stream_create"))
        .await
        .unwrap();

    let mut index = 0;

    let mut set = HashSet::new();
    let iterations = 10000;

    while let Some(Ok(record)) = stream
        .next()
        .instrument(debug_span!("stream_next", record_num = index))
        .await
    {
        let value = String::from_utf8_lossy(record.value())
            .parse::<usize>()
            .expect("Unable to parse");
        println!("Consuming {:<5}: was consumed: {:?}", index, value);

        assert!((0..iterations).contains(&value));

        set.insert(value);
        index += 1;
        if index == iterations {
            break;
        }
    }
    assert_eq!(set.len(), iterations)
}

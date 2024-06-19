use std::collections::HashSet;
use fluvio::consumer::ConsumerConfigExtBuilder;
use fluvio_test_util::test_meta::environment::EnvDetail;
use fluvio_test_util::test_runner::test_driver::TestDriver;
use futures_lite::StreamExt;
use fluvio::Offset;

use super::MyTestCase;

pub async fn consumer_stream(test_driver: &TestDriver, option: MyTestCase) {
    let config = ConsumerConfigExtBuilder::default()
        .topic(option.environment.base_topic_name())
        .offset_start(Offset::beginning())
        .build()
        .expect("config");
    let mut stream = test_driver.get_consumer_with_config(config).await;
    let mut index = 0;

    let mut set = HashSet::new();
    let iterations = 10000;

    while let Some(Ok(record)) = stream.next().await {
        let value = String::from_utf8_lossy(record.value())
            .parse::<usize>()
            .expect("Unable to parse");
        println!("Consuming {index:<5}: was consumed: {value:?}");

        assert!((0..iterations).contains(&value));

        set.insert(value);
        index += 1;
        if index == iterations {
            break;
        }
    }
    assert_eq!(set.len(), iterations)
}

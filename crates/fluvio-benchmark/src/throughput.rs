use async_std::{task::spawn, future::timeout, prelude::FutureExt};

use crate::{Setup, DEFAULT_TIMEOUT};

pub async fn run_throughput_test(setup: Setup) {
    let (mut producer, mut consumer) = setup;
    // producer.produce().await;
    // consumer.consume().await;
    let producer_jh = spawn(timeout(DEFAULT_TIMEOUT, producer.produce()));
    let consumer_jh = spawn(timeout(DEFAULT_TIMEOUT, consumer.consume()));
    let (producer_result, consumer_result) = producer_jh.join(consumer_jh).await;
    producer_result.unwrap();
    consumer_result.unwrap();
}
// pub async fn produce(producer: TopicProducer, producer_data: Vec<String>) {
//     for record_data in producer_data {
//         producer.send(RecordKey::NULL, record_data).await.unwrap();
//     }
//     producer.flush().await.unwrap();
// }

// pub async fn consume(consumer: PartitionConsumer, consumer_data: Vec<String>) {
//     // We produce and consume one record to warm_up
//     let mut stream = consumer.stream(Offset::absolute(1).unwrap()).await.unwrap();

//     for expected in consumer_data {
//         let record = stream.next().await.unwrap().unwrap();
//         let value = String::from_utf8_lossy(record.value());
//         assert_eq!(value, expected);
//     }
// }

use std::time::Instant;
use async_std::{stream::StreamExt, future::timeout};
use fluvio::{RecordKey, Offset};

use crate::{Setup, DEFAULT_TIMEOUT};

pub async fn run_latency_test(setup: Setup) {
    let now = Instant::now();
    timeout(DEFAULT_TIMEOUT, async {
        let ((producer, producer_data), (consumer, consumer_data)) = setup;
        producer
            .send(RecordKey::NULL, producer_data.into_iter().next().unwrap())
            .await
            .unwrap();
        producer.flush().await.unwrap();
        let record = consumer
            .stream(Offset::absolute(1).unwrap())
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap();
        let value = String::from_utf8_lossy(record.value());
        assert_eq!(value, consumer_data[0]);
    })
    .await
    .unwrap();
    println!("{:?}", now.elapsed());
}

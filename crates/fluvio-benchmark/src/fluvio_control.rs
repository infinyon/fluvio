use std::collections::VecDeque;

use async_std::stream::StreamExt;
use fluvio::{TopicProducer, RecordKey, PartitionConsumer, Offset};

pub struct Producer {
    pub producer: TopicProducer,
    pub data: VecDeque<String>,
    pub records_per_batch: usize,
}

impl Producer {
    pub async fn produce(mut self) {
        for _ in 0..self.records_per_batch {
            self.producer
                .send(RecordKey::NULL, self.data.pop_front().unwrap())
                .await
                .unwrap();
        }
        self.producer.flush().await.unwrap();
    }
}

pub struct Consumer {
    pub consumer: PartitionConsumer,
    pub data: VecDeque<String>,
    pub records_per_batch: usize,
    pub offset: i64,
}
impl Consumer {
    pub async fn consume(mut self) {
        let mut stream = self
            .consumer
            .stream(Offset::absolute(self.offset).unwrap())
            .await
            .unwrap();
        for _ in 0..self.records_per_batch {
            let record = stream.next().await.unwrap().unwrap();
            let value = String::from_utf8_lossy(record.value());
            let expected = self.data.pop_front().unwrap();
            assert_eq!(value, expected);
        }
    }
}

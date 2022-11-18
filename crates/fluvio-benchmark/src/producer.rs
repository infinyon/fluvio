use std::collections::VecDeque;

use fluvio::{TopicProducer, RecordKey};

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
            self.producer.flush().await.unwrap();
        }
        // self.producer.flush().await.unwrap();
    }
}

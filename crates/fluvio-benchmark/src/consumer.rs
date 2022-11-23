use std::collections::VecDeque;
use async_std::stream::StreamExt;
use fluvio::{PartitionConsumer, Offset};

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

    pub async fn consume_no_check(self) {
        let mut stream = self
            .consumer
            .stream(Offset::absolute(self.offset).unwrap())
            .await
            .unwrap();
        for _ in 0..self.records_per_batch {
            let _ = stream.next().await.unwrap().unwrap();
        }
    }
}

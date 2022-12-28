use fluvio_protocol::{Decoder, Encoder};
use fluvio_protocol::record::BatchRecords;

use crate::batch::FileBatchStream;
use crate::batch::FileBatchPos;

pub type BatchHeaderStream = FileBatchStream<FileEmptyRecords>;

pub type BatchHeaderPos = FileBatchPos<FileEmptyRecords>;

#[derive(Default, Debug, Encoder, Decoder)]
pub struct FileEmptyRecords {}

impl BatchRecords for FileEmptyRecords {
    fn remainder_bytes(&self, _remainder: usize) -> usize {
        0
    }
}

#[cfg(test)]
mod tests {

    use std::env::temp_dir;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Instant;

    use tracing::debug;

    use fluvio_protocol::record::Offset;
    use flv_util::fixture::{ensure_new_dir};

    use crate::fixture::BatchProducer;
    use crate::mut_records::MutFileRecords;
    use crate::config::ReplicaConfig;
    use crate::records::FileRecords;
    use super::BatchHeaderStream;

    fn default_option(base_dir: PathBuf) -> ReplicaConfig {
        ReplicaConfig {
            base_dir,
            segment_max_bytes: 1000,
            ..Default::default()
        }
    }

    #[fluvio_future::test]
    async fn test_decode_batch_header_simple() {
        const BASE_OFFSET: Offset = 200;
        const PRODUCER_ID: i64 = 12;

        let test_dir = temp_dir().join("header-simpl");
        ensure_new_dir(&test_dir).expect("new");

        let options = default_option(test_dir.clone());

        let mut builder = BatchProducer::builder()
            .base_offset(BASE_OFFSET)
            .producer_id(PRODUCER_ID)
            .build()
            .expect("build");

        let mut msg_sink = MutFileRecords::create(BASE_OFFSET, Arc::new(options.into()))
            .await
            .expect("create");

        let batch = builder.batch();
        assert_eq!(batch.get_last_offset(), BASE_OFFSET + 1);
        // write a batch with 2 records
        msg_sink.write_batch(&batch).await.expect("write");

        let log_path = msg_sink.get_path().to_owned();
        drop(msg_sink);

        let mut stream = BatchHeaderStream::open(log_path).await.expect("open");

        let file_pos = stream.try_next().await.expect("ok").expect("some");
        assert_eq!(stream.get_pos(), 79);
        assert_eq!(file_pos.get_pos(), 0);
        let batch = file_pos.inner();

        assert_eq!(batch.get_header().producer_id, 12);
        assert_eq!(batch.get_base_offset(), BASE_OFFSET);
        assert_eq!(batch.get_last_offset(), BASE_OFFSET + 1);
        assert!((stream.try_next().await.expect("ok")).is_none());
    }

    #[fluvio_future::test]
    async fn test_decode_batch_header_multiple() {
        const BASE_OFFSET: Offset = 200;
        const PRODUCER_ID: i64 = 25;

        let test_dir = temp_dir().join("header_multiple");
        ensure_new_dir(&test_dir).expect("new");

        let options = default_option(test_dir.clone());

        let mut msg_sink = MutFileRecords::create(BASE_OFFSET, Arc::new(options.into()))
            .await
            .expect("create");

        let mut builder = BatchProducer::builder()
            .base_offset(BASE_OFFSET)
            .producer_id(PRODUCER_ID)
            .build()
            .expect("build");

        // writing 2 batches of 2 records = 4 records
        msg_sink.write_batch(&builder.batch()).await.expect("write");

        // since mut records doesn't write base offset, we need to set manually
        let test_batch = builder.batch();

        assert_eq!(test_batch.get_header().producer_id, PRODUCER_ID);
        msg_sink.write_batch(&test_batch).await.expect("write");

        let log_path = msg_sink.get_path().to_owned();
        drop(msg_sink);

        let mut stream = BatchHeaderStream::open(log_path).await.expect("open");

        let batch_pos1 = stream.try_next().await.expect("ok").expect("batch");
        assert_eq!(stream.get_pos(), 79);
        assert_eq!(batch_pos1.get_pos(), 0);
        let batch1 = batch_pos1.inner();

        assert_eq!(batch1.get_base_offset(), BASE_OFFSET);
        assert_eq!(batch1.get_header().producer_id, PRODUCER_ID);

        let batch_pos2 = stream.try_next().await.expect("ok").expect("batch");
        assert_eq!(stream.get_pos(), 158);
        assert_eq!(batch_pos2.get_pos(), 79);
        let batch2 = batch_pos2.inner();

        assert_eq!(batch2.get_base_offset(), BASE_OFFSET + 2);
        assert_eq!(batch2.get_header().producer_id, PRODUCER_ID);

        assert!((stream.try_next().await.expect("ok")).is_none());
    }

    //#[fluvio_future::test]
    #[allow(unused)]
    async fn test_code_perf() {
        let mut stream = BatchHeaderStream::open(
            "/tmp/fluvio-large-data/spu-logs-5002/longevity-0/00000000000000000000.log",
        )
        .await
        .expect("open");

        let mut counter = 0;
        println!("starting test");
        let write_time = Instant::now();
        let mut last_base_offset = 0;
        // let mut records: i32 = 0;
        while let Some(batch) = stream.try_next().await.expect("ok") {
            counter += 1;
            //   println!("offset delta: {}",batch.get_batch().get_last_offset());
            //  records += batch.get_batch().get_records().len() as i32;
            //   if counter > 10 {
            //       break;
            //   }
            //  println!("pos: {}",stream.get_pos());
            //  println!("base_offset: {}",batch.get_batch().get_base_offset());
            last_base_offset = batch.inner().get_base_offset();
        }

        let time = write_time.elapsed();
        println!(
            "took: {:#?}, count: {}, pos = {},base_offset={}",
            time,
            counter,
            stream.get_pos(),
            last_base_offset
        );
    }

    //#[fluvio_future::test]
    #[allow(unused)]
    async fn test_find_position() {
        let mut header_stream = BatchHeaderStream::open("/tmp/bad_header.log")
            .await
            .expect("open");

        header_stream.set_absolute(55205702).await.expect("set");

        let offset_seek = 46612;

        let mut found = true;
        while let Some(batch_pos) = header_stream.try_next().await.expect("ok") {
            let pos = batch_pos.get_pos();
            let batch = batch_pos.inner();
            debug!(
                base_offset = batch.base_offset,
                batch_len = batch.batch_len,
                last_offset = batch.get_last_offset(),
                "batch_pos"
            );
            let last_offset = batch.get_last_offset();
            if last_offset >= offset_seek {
                debug!(last_offset, "found batch last offset");
                found = true;
                break;
            } else {
                debug!(last_offset, "skipping batch end offset");
            }
        }

        assert!(found);
    }
}

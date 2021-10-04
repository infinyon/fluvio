use std::io::Error as IoError;

use dataplane::core::{Version, Decoder, Encoder};
use dataplane::bytes::Buf;
use dataplane::bytes::BufMut;
use dataplane::batch::BatchRecords;

use crate::batch::FileBatchStream;
use crate::batch::FileBatchPos;

pub type BatchHeaderStream = FileBatchStream<FileEmptyRecords>;

pub type BatchHeaderPos = FileBatchPos<FileEmptyRecords>;

#[derive(Default, Debug)]
pub struct FileEmptyRecords {}

impl BatchRecords for FileEmptyRecords {
    fn remainder_bytes(&self, _remainder: usize) -> usize {
        0
    }
}

// nothing to decode for header
impl Decoder for FileEmptyRecords {
    fn decode<T>(&mut self, _src: &mut T, _version: Version) -> Result<(), IoError>
    where
        T: Buf,
    {
        Ok(())
    }
}

// nothing to do decode for header
impl Encoder for FileEmptyRecords {
    fn write_size(&self, _versio: Version) -> usize {
        0
    }

    fn encode<T>(&self, _dest: &mut T, _version: Version) -> Result<(), IoError>
    where
        T: BufMut,
    {
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use std::env::temp_dir;
    use std::path::PathBuf;
    use std::time::Instant;

    use dataplane::Offset;
    use dataplane::batch::Batch;
    use flv_util::fixture::{ensure_new_dir};
    use dataplane::fixture::create_batch;
    use dataplane::fixture::create_batch_with_producer;

    use crate::mut_records::MutFileRecords;
    use crate::config::ConfigOption;
    use crate::records::FileRecords;
    use super::BatchHeaderStream;

    fn default_option(base_dir: PathBuf) -> ConfigOption {
        ConfigOption {
            base_dir,
            segment_max_bytes: 1000,
            ..Default::default()
        }
    }

    fn create_batch_with(base_offset: Offset) -> Batch {
        let mut batch = create_batch();
        batch.set_base_offset(base_offset);
        batch
    }

    #[fluvio_future::test]
    async fn test_decode_batch_header_simple() {
        let test_dir = temp_dir().join("header-simpl");
        ensure_new_dir(&test_dir).expect("new");

        let options = default_option(test_dir.clone());

        let mut msg_sink = MutFileRecords::create(200, &options).await.expect("create");

        let batch = create_batch_with(200);
        assert_eq!(batch.get_last_offset(), 201);
        // write a batch with 2 records
        msg_sink
            .write_batch(&batch)
            .await
            .expect("write");

        let log_path = msg_sink.get_path().to_owned();
        drop(msg_sink);

        let mut stream = BatchHeaderStream::open(log_path).await.expect("open");

        let file_pos = stream.next().await.expect("some");
        assert_eq!(stream.get_pos(), 79);
        assert_eq!(file_pos.get_pos(), 0);
        let batch = file_pos.get_batch();
        assert_eq!(batch.get_header().producer_id, 12);
        assert_eq!(batch.get_base_offset(), 200);
        assert_eq!(batch.get_last_offset(), 201);
        assert!((stream.next().await).is_none());
    }

    #[fluvio_future::test]
    async fn test_decode_batch_header_multiple() {
        let test_dir = temp_dir().join("header_multiple");
        ensure_new_dir(&test_dir).expect("new");

        let options = default_option(test_dir.clone());

        let mut msg_sink = MutFileRecords::create(200, &options).await.expect("create");

        // writing 2 batches of 2 records = 4 records
        msg_sink
            .write_batch(&mut create_batch_with(200))
            .await
            .expect("write");

        let mut test_batch = create_batch_with_producer(25, 2);
        test_batch.set_base_offset(202);
        assert_eq!(test_batch.get_header().producer_id, 25);
        msg_sink
            .write_batch(&test_batch)
            .await
            .expect("write");

        let log_path = msg_sink.get_path().to_owned();
        drop(msg_sink);

        let mut stream = BatchHeaderStream::open(log_path).await.expect("open");

        let batch_pos1 = stream.next().await.expect("batch");
        assert_eq!(stream.get_pos(), 79);
        assert_eq!(batch_pos1.get_pos(), 0);
        let batch1 = batch_pos1.get_batch();
        assert_eq!(batch1.get_base_offset(), 200);
        assert_eq!(batch1.get_header().producer_id, 12);

        let batch_pos2 = stream.next().await.expect("batch");
        assert_eq!(stream.get_pos(), 158);
        assert_eq!(batch_pos2.get_pos(), 79);
        let batch2 = batch_pos2.get_batch();
        assert_eq!(batch2.get_base_offset(), 202);
        assert_eq!(batch2.get_header().producer_id, 25);

        assert!((stream.next().await).is_none());
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
        while let Some(batch) = stream.next().await {
            counter = counter + 1;
            //   println!("offset delta: {}",batch.get_batch().get_last_offset());
            //  records += batch.get_batch().get_records().len() as i32;
            //   if counter > 10 {
            //       break;
            //   }
            //  println!("pos: {}",stream.get_pos());
            //  println!("base_offset: {}",batch.get_batch().get_base_offset());
            last_base_offset = batch.get_batch().get_base_offset();
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
}

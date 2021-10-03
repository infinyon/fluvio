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

    #[fluvio_future::test]
    async fn test_decode_batch_header_simple() {
        let test_dir = temp_dir().join("header-simpl");
        ensure_new_dir(&test_dir).expect("new");

        let options = default_option(test_dir.clone());

        let mut msg_sink = MutFileRecords::create(200, &options).await.expect("create");

        let batch = create_batch();
        assert_eq!(batch.get_last_offset(), 1);
        // write a batch with 2 records
        msg_sink
            .write_batch(&mut create_batch())
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

        let mut msg_sink = MutFileRecords::create(201, &options).await.expect("create");

        // writing 2 batches of 2 records = 4 records
        msg_sink
            .write_batch(&mut create_batch())
            .await
            .expect("write");
        msg_sink
            .write_batch(&mut create_batch_with_producer(25, 2))
            .await
            .expect("write");

        let log_path = msg_sink.get_path().to_owned();
        drop(msg_sink);

        let mut stream = BatchHeaderStream::open(log_path).await.expect("open");

        let batch_pos1 = stream.next().await.expect("batch");
        assert_eq!(stream.get_pos(), 79);
        assert_eq!(batch_pos1.get_pos(), 0);
        let batch1 = batch_pos1.get_batch();
        assert_eq!(batch1.get_base_offset(), 201);
        assert_eq!(batch1.get_header().producer_id, 12);

        let batch_pos2 = stream.next().await.expect("batch");
        assert_eq!(stream.get_pos(), 158);
        assert_eq!(batch_pos2.get_pos(), 79);
        let batch2 = batch_pos2.get_batch();
        assert_eq!(batch2.get_header().producer_id, 25);

        assert!((stream.next().await).is_none());
    }
}

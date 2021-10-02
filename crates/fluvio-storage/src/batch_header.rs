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

    use flv_util::fixture::ensure_clean_file;
    use dataplane::fixture::create_batch;
    use dataplane::fixture::create_batch_with_producer;

    use crate::mut_records::MutFileRecords;
    use crate::config::ConfigOption;
    use super::BatchHeaderStream;

    #[allow(unused)]
    const TEST_FILE_NAME: &str = "00000000000000000200.log"; // for offset 200

    #[allow(unused)]
    fn default_option() -> ConfigOption {
        ConfigOption {
            base_dir: temp_dir(),
            segment_max_bytes: 1000,
            ..Default::default()
        }
    }

    #[allow(unused, clippy::unnecessary_mut_passed)]
    //#[fluvio_future::test]
    async fn test_decode_batch_header_simple() {
        let test_file = temp_dir().join(TEST_FILE_NAME);
        ensure_clean_file(&test_file);

        let options = default_option();

        let mut msg_sink = MutFileRecords::create(200, &options)
            .await
            .expect("create sink");

        msg_sink
            .write_batch(&mut create_batch())
            .await
            .expect("send batch");

        let mut stream = BatchHeaderStream::open(test_file).await.expect("open");

        let batch_header_opt = stream.next().await;
        assert!(batch_header_opt.is_some());
        let batch = batch_header_opt.expect("some");
        let header = batch.get_batch().get_header();
        assert_eq!(header.producer_id, 12);
    }

    #[allow(unused)]
    const TEST_FILE_NAME2: &str = "00000000000000000201.log"; // for offset 200

    #[allow(unused, clippy::unnecessary_mut_passed)]
    //#[fluvio_future::test]
    async fn test_decode_batch_header_multiple() {
        let test_file = temp_dir().join(TEST_FILE_NAME2);
        ensure_clean_file(&test_file);

        let options = default_option();

        let mut msg_sink = MutFileRecords::create(201, &options).await.expect("create");

        msg_sink
            .write_batch(&mut create_batch())
            .await
            .expect("write");
        msg_sink
            .write_batch(&mut create_batch_with_producer(25, 2))
            .await
            .expect("write");

        let mut stream = BatchHeaderStream::open(test_file).await.expect("open");

        let batch_pos1 = stream.next().await.expect("batch");
        assert_eq!(batch_pos1.get_batch().get_header().producer_id, 12);
        assert_eq!(batch_pos1.get_pos(), 0);
        let batch_pos2 = stream.next().await.expect("batch");
        assert_eq!(batch_pos2.get_batch().get_header().producer_id, 25);
        assert_eq!(batch_pos2.get_pos(), 79); // 2 records
        assert!((stream.next().await).is_none());
    }
}

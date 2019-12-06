
use std::io::Error as IoError;
use std::task::Context;
use std::task::Poll;

use futures::Future;
use futures::Stream;
use std::pin::Pin;

use pin_utils::pin_mut;

use kf_protocol::Version;
use kf_protocol::bytes::Buf;
use kf_protocol::bytes::BufMut;
use kf_protocol::api::BatchRecords;
use kf_protocol::Decoder;
use kf_protocol::Encoder;

use crate::batch::FileBatchStream;
use crate::batch::FileBatchPos;

pub type BatchHeaderStream = FileBatchStream<FileEmptyRecords>;

pub type BatchHeaderPos = FileBatchPos<FileEmptyRecords>;


#[derive(Default,Debug)]
pub struct FileEmptyRecords {

}

impl BatchRecords for FileEmptyRecords {

     fn remainder_bytes(&self,_remainder: usize ) -> usize {
        0
    }
}

// nothing to decode for header
impl Decoder for FileEmptyRecords   {

    fn decode<T>(&mut self, _src: &mut T,_version:  Version) -> Result<(), IoError> where T: Buf,
    {
        Ok(())
    }
}


// nothing to do decode for header
impl Encoder for FileEmptyRecords   {

    fn write_size(&self,_versio: Version) -> usize {
        0
    }

    fn encode<T>(&self, _dest: &mut T,_version: Version) -> Result<(), IoError> where T: BufMut
    {
        Ok(())
    }
}


/// need to create separate implemention of batch stream
/// for specific implemetnation due to problem with compiler
impl Stream for FileBatchStream<FileEmptyRecords> {

    type Item = BatchHeaderPos; 

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {

        let ft = self.inner_next();
        pin_mut!(ft);
        ft.poll(cx)

    }

}



#[cfg(test)]
mod tests {

    use std::env::temp_dir;

    use futures::sink::SinkExt;
    use futures::stream::StreamExt;

    use future_aio::fs::file_util;

    use crate::fixture::create_batch;
    use crate::fixture::create_batch_with_producer;
    use crate::fixture::ensure_clean_file;
    use crate::mut_records::MutFileRecords;
    use crate::ConfigOption;
    use crate::StorageError;

    use super::BatchHeaderStream;
    use super::BatchHeaderPos;

    const TEST_FILE_NAME: &str = "00000000000000000200.log"; // for offset 200

    fn default_option() -> ConfigOption {
        ConfigOption {
            base_dir: temp_dir(),
            segment_max_bytes: 1000,
            ..Default::default()
        }
    }

    //#[test_async]
    async fn test_decode_batch_header_simple() -> Result<(), StorageError> {
        let test_file = temp_dir().join(TEST_FILE_NAME);
        ensure_clean_file(&test_file);

        let options = default_option();

        let mut msg_sink = MutFileRecords::create(200, &options).await.expect("create sink");

        msg_sink.send(create_batch()).await.expect("send batch");

        let mut file = file_util::open(&test_file).await.expect("open test file");

        let batch_res = BatchHeaderPos::from(&mut file, 0).await.expect("open header");

        if let Some(batch) = batch_res {
            let header = batch.get_batch().get_header();
            assert_eq!(header.producer_id, 12);
        } else {
            assert!(false, "batch not found");
        }

        Ok(())
    }

    const TEST_FILE_NAME2: &str = "00000000000000000201.log"; // for offset 200

    //#[test_async]
    async fn test_decode_batch_header_multiple() -> Result<(), StorageError> {
        let test_file = temp_dir().join(TEST_FILE_NAME2);
        ensure_clean_file(&test_file);

        let options = default_option();

        let mut msg_sink = MutFileRecords::create(201, &options).await?;

        msg_sink.send(create_batch()).await?;
        msg_sink.send(create_batch_with_producer(25, 2)).await?;

        let file = file_util::open(&test_file).await?;

        let mut stream = BatchHeaderStream::new(file);

        let batch_pos1 = stream.next().await.expect("batch");
        assert_eq!(batch_pos1.get_batch().get_header().producer_id, 12);
        assert_eq!(batch_pos1.get_pos(), 0);
        let batch_pos2 = stream.next().await.expect("batch");
        assert_eq!(batch_pos2.get_batch().get_header().producer_id, 25);
        assert_eq!(batch_pos2.get_pos(), 79); // 2 records
        assert!((stream.next().await).is_none());

        Ok(())
    }

}

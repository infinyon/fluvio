use std::pin::Pin;
use std::io::Error as IoError;
use std::task::Context;
use std::task::Poll;

use futures::sink::Sink;
use futures::ready;
use log::debug;
use log::trace;
use pin_utils::pin_mut;
use pin_utils::unsafe_pinned;

use future_aio::fs::FileSink;
use future_aio::fs::AsyncFile;
use future_aio::fs::AsyncFileSlice;
use future_aio::fs::FileSinkError;
use future_aio::fs::FileSinkOption;
use kf_protocol::api::DefaultBatch;
use kf_protocol::api::Offset;
use kf_protocol::api::Size;
use kf_protocol::Encoder;

use crate::util::generate_file_name;
use crate::validator::validate;
use crate::validator::LogValidationError;
use crate::ConfigOption;
use crate::StorageError;
use crate::records::FileRecords;

pub const MESSAGE_LOG_EXTENSION: &'static str = "log";


/// Can append new batch to file
pub struct MutFileRecords {
    base_offset: Offset,
    item_last_offset_delta: Size,         
    f_sink: FileSink<Vec<u8>>,
}

impl MutFileRecords {
    unsafe_pinned!(f_sink: FileSink<Vec<u8>>);

    pub async fn create(
        base_offset: Offset,
        option: &ConfigOption,
    ) -> Result<MutFileRecords, FileSinkError> {
        let sink_option = FileSinkOption {
            max_len: Some(option.segment_max_bytes as u64),
        };
        let log_path = generate_file_name(&option.base_dir, base_offset, MESSAGE_LOG_EXTENSION);
        debug!("creating log at: {}", log_path.display());
        let f_sink = FileSink::open_append(&log_path, sink_option).await?;
        Ok(MutFileRecords {
            base_offset,
            f_sink,
            item_last_offset_delta: 0,
        })
    }

    pub async fn open(
        base_offset: Offset,
        option: &ConfigOption,
    ) -> Result<MutFileRecords, StorageError> {
        let log_path = generate_file_name(&option.base_dir, base_offset, MESSAGE_LOG_EXTENSION);
        trace!("opening commit log at: {}", log_path.display());

        let sink_option = FileSinkOption {
            max_len: Some(option.segment_max_bytes as u64),
        };

        let f_sink = FileSink::open_append(log_path, sink_option).await?;
        Ok(MutFileRecords {
            base_offset,
            f_sink,
            item_last_offset_delta: 0
        })
    }

    pub fn get_base_offset(&self) -> Offset {
        self.base_offset
    }

    pub async fn validate(&mut self) -> Result<Offset, LogValidationError> {
        validate(self.f_sink.get_mut_writer()).await
    }

    pub fn get_pos(&self) -> Size {
        self.f_sink.get_current_len() as Size
    }

    pub fn get_pending_batch_len(&self) -> Size {
        self.f_sink.get_pending_len() as Size
    }


    pub fn get_item_last_offset_delta(&self) -> Size {
        self.item_last_offset_delta
    }

    
    
    
}

impl FileRecords for MutFileRecords {

    fn get_base_offset(&self) -> Offset {
        self.base_offset
    }

    fn get_file(&self) -> &AsyncFile {
        &self.f_sink.get_writer()
    }

    
    fn as_file_slice(&self, start: Size) -> Result<AsyncFileSlice,IoError> {
        self.f_sink.slice_from(start as u64, self.f_sink.get_current_len() - start as u64)
    }


    fn as_file_slice_from_to(&self, start: Size, len: Size) -> Result<AsyncFileSlice,IoError> {
        self.f_sink.slice_from(start as u64, len as u64)
    }
    
}

impl Unpin for MutFileRecords {}

impl Sink<DefaultBatch> for MutFileRecords {
    type Error = StorageError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.f_sink().poll_ready(cx).map_err(|err| err.into())
    }

    fn start_send(mut self: Pin<&mut Self>, item: DefaultBatch) -> Result<(), Self::Error> {
        debug!("writing batch {:#?}", item.get_header());
        self.item_last_offset_delta = item.get_last_offset_delta();
        let mut buffer: Vec<u8> = vec![];
        item.encode(&mut buffer,0)?;
        let sink = &mut self.as_mut().f_sink();
        pin_mut!(sink);
        trace!("writing {} bytes", buffer.len());
        match sink.start_send(buffer) {
            Ok(_) => Ok(()),
            Err(err) => match err {
                FileSinkError::MaxLenReached => Err(StorageError::NoRoom(item)),
                _ => Err(err.into()),
            },
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        let f_sink = self.as_mut().f_sink();
        let flush_poll: Poll<Result<(), Self::Error>> =
            f_sink.poll_flush(cx).map_err(|err| err.into());
        ready!(flush_poll)?;
        debug!("flushed log with pos: {}", self.get_pos());
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.f_sink().poll_close(cx).map_err(|err| err.into())
    }
}

#[cfg(test)]
mod tests {

    use futures::sink::SinkExt;
    use std::env::temp_dir;
    use std::io::Cursor;

    use future_helper::test_async;
    use kf_protocol::api::DefaultBatch;
    use kf_protocol::Decoder;

    use super::MutFileRecords;
    use super::StorageError;
    use crate::fixture::create_batch;
    use crate::fixture::ensure_clean_file;
    use crate::fixture::read_bytes_from_file;
    use crate::ConfigOption;

    const TEST_FILE_NAME: &str = "00000000000000000100.log"; // for offset 100

    #[test_async]
    async fn test_sink_msg() -> Result<(), StorageError> {
        let test_file = temp_dir().join(TEST_FILE_NAME);
        ensure_clean_file(&test_file);

        let options = ConfigOption {
            base_dir: temp_dir(),
            segment_max_bytes: 1000,
            ..Default::default()
        };
        let mut msg_sink = MutFileRecords::create(100, &options).await?;

        msg_sink.send(create_batch()).await?;

        let bytes = read_bytes_from_file(&test_file)?;
        assert_eq!(bytes.len(), 79, "sould be 70 bytes");

        let batch = DefaultBatch::decode_from(&mut Cursor::new(bytes),0)?;
        assert_eq!(batch.get_header().magic, 2, "check magic");
        assert_eq!(batch.records.len(), 2);

        msg_sink.send(create_batch()).await?;
        let bytes = read_bytes_from_file(&test_file)?;
        assert_eq!(bytes.len(), 158, "sould be 158 bytes");

        let old_msg_sink = MutFileRecords::open(100, &options).await?;
        assert_eq!(old_msg_sink.get_base_offset(), 100);

        Ok(())
    }

}

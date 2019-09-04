use std::io::Error as IoError;
use std::fmt;

use log::trace;

use future_aio::fs::AsyncFileSlice;
use future_aio::BufMut;
use future_aio::BytesMut;
use kf_protocol::Version;
use kf_protocol::Encoder;
use kf_protocol::Decoder;
use kf_protocol::bytes::Buf;
use kf_protocol::message::fetch::KfFetchResponse;
use kf_protocol::message::fetch::KfFetchRequest;
use kf_protocol::message::fetch::FetchableTopicResponse;
use kf_protocol::message::fetch::FetchablePartitionResponse;

use crate::StoreValue;
use crate::FileWrite;

pub type FileFetchResponse = KfFetchResponse<KfFileRecordSet>;
pub type FileTopicResponse = FetchableTopicResponse<KfFileRecordSet>;
pub type FilePartitionResponse = FetchablePartitionResponse<KfFileRecordSet>;

#[derive(Default, Debug)]
pub struct KfFileRecordSet(AsyncFileSlice);

pub type KfFileFetchRequest = KfFetchRequest<KfFileRecordSet>;



impl fmt::Display for KfFileRecordSet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,"pos: {} len: {}",self.position(),self.len())
    }
}


impl KfFileRecordSet {
    pub fn position(&self) -> u64 {
        self.0.position()
    }

    pub fn len(&self) -> usize {
        self.0.len() as usize
    }

    pub fn raw_slice(&self) -> &AsyncFileSlice {
        &self.0
    }
}

impl From<AsyncFileSlice> for KfFileRecordSet {
    fn from(slice: AsyncFileSlice) -> Self {
        Self(slice)
    }
}

impl Encoder for KfFileRecordSet {
    fn write_size(&self, _version: Version) -> usize {
        self.len() + 4 // include header
    }

    fn encode<T>(&self, _src: &mut T, _version: Version) -> Result<(), IoError>
    where
        T: BufMut,
    {
        unimplemented!("file slice cannot be encoded in the ButMut")
    }
}

impl Decoder for KfFileRecordSet {
    fn decode<T>(&mut self, _src: &mut T, _version: Version) -> Result<(), IoError>
    where
        T: Buf,
    {
        unimplemented!("file slice cannot be decoded in the ButMut")
    }
}

impl FileWrite for KfFileRecordSet {
    fn file_encode<'a: 'b, 'b>(
        &'a self,
        dest: &mut BytesMut,
        data: &'b mut Vec<StoreValue<'a>>,
        version: Version,
    ) -> Result<(), IoError> {
        let len: i32 = self.len() as i32;
        trace!("KfFileRecordSet encoding file slice len: {}", len);
        len.encode(dest, version)?;
        let bytes = dest.take().freeze();
        data.push(StoreValue::Bytes(bytes));
        data.push(StoreValue::FileSlice(&self.raw_slice()));
        Ok(())
    }
}

impl FileWrite for FileFetchResponse {
    fn file_encode<'a: 'b, 'b>(
        &'a self,
        src: &mut BytesMut,
        data: &'b mut Vec<StoreValue<'a>>,
        version: Version,
    ) -> Result<(), IoError> {
        trace!("file encoding FileFetchResponse");
        trace!("encoding throttle_time_ms {}", self.throttle_time_ms);
        self.throttle_time_ms.encode(src, version)?;
        trace!("encoding error code {:#?}", self.error_code);
        self.error_code.encode(src, version)?;
        trace!("encoding session code {}", self.session_id);
        self.session_id.encode(src, version)?;
        trace!("encoding topics len: {}", self.topics.len());
        self.topics.file_encode(src, data, version)?;
        Ok(())
    }
}

impl FileWrite for FileTopicResponse {
    fn file_encode<'a: 'b, 'b>(
        &'a self,
        src: &mut BytesMut,
        data: &'b mut Vec<StoreValue<'a>>,
        version: Version,
    ) -> Result<(), IoError> {
        trace!("file encoding fetch topic response");
        self.name.encode(src, version)?;
        self.partitions.file_encode(src, data, version)?;
        Ok(())
    }
}

impl FileWrite for FilePartitionResponse {
    fn file_encode<'a: 'b, 'b>(
        &'a self,
        src: &mut BytesMut,
        data: &'b mut Vec<StoreValue<'a>>,
        version: Version,
    ) -> Result<(), IoError> {
        trace!("file encoding fetch partition response");
        self.partition_index.encode(src, version)?;
        self.error_code.encode(src, version)?;
        self.high_watermark.encode(src, version)?;
        self.last_stable_offset.encode(src, version)?;
        self.log_start_offset.encode(src, version)?;
        self.aborted.encode(src, version)?;
        self.records.file_encode(src, data, version)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {

    use std::io::Error as IoError;
    use std::env::temp_dir;
    use std::net::SocketAddr;
    use std::time::Duration;

    use log::debug;
    use futures::io::AsyncWriteExt;
    use futures::future::join;
    use futures::stream::StreamExt;

    use future_helper::test_async;
    use future_helper::sleep;
    use kf_protocol::Encoder;
    use kf_protocol::api::Request;
    use kf_protocol::api::ResponseMessage;
    use kf_protocol::api::RequestMessage;
    use kf_protocol::api::DefaultBatch;
    use kf_protocol::api::DefaultRecord;
    use kf_protocol::message::fetch::DefaultKfFetchRequest;
    use future_aio::fs::AsyncFile;
    use future_aio::net::AsyncTcpListener;
    use utils::fixture::ensure_clean_file;
    use crate::KfSocket;
    use crate::KfSocketError;
    use crate::FileFetchResponse;
    use crate::KfFileFetchRequest;
    use crate::FilePartitionResponse;
    use crate::FileTopicResponse;

    /// create sample batches with message
    fn create_batches(records: u16) -> DefaultBatch {
        let mut batches = DefaultBatch::default();
        let header = batches.get_mut_header();
        header.magic = 2;
        header.producer_id = 20;
        header.producer_epoch = -1;

        for i in 0..records {
            let msg = format!("record {}", i);
            let record: DefaultRecord = msg.into();
            batches.add_record(record);
        }
        batches
    }

    async fn setup_batch_file() -> Result<(), IoError> {
        let test_file_path = temp_dir().join("batch_fetch");
        ensure_clean_file(&test_file_path);
        debug!("creating test file: {:#?}", test_file_path);
        let mut file = AsyncFile::create(&test_file_path).await?;
        let batch = create_batches(2);
        let bytes = batch.as_bytes(0)?;
        file.write_all(bytes.as_ref()).await?;
        Ok(())
    }

    async fn test_server(addr: SocketAddr) -> Result<(), KfSocketError> {
        let listener = AsyncTcpListener::bind(&addr)?;
        debug!("server is running");
        let mut incoming = listener.incoming();
        let incoming_stream = incoming.next().await;
        debug!("server: got connection");
        let incoming_stream = incoming_stream.expect("next").expect("unwrap again");
        let mut socket: KfSocket = incoming_stream.into();

        let fetch_request: Result<RequestMessage<KfFileFetchRequest>, KfSocketError> = socket
            .get_mut_stream()
            .next_request_item()
            .await
            .expect("next value");
        let request = fetch_request?;
        debug!("received fetch request: {:#?}", request);

        let test_file_path = temp_dir().join("batch_fetch");
        debug!("opening file test file: {:#?}", test_file_path);
        let file = AsyncFile::open(&test_file_path).await?;

        let mut response = FileFetchResponse::default();
        let mut topic_response = FileTopicResponse::default();
        let mut part_response = FilePartitionResponse::default();
        part_response.partition_index = 10;
        part_response.records = file.as_slice(0, None).await?.into();
        topic_response.partitions.push(part_response);
        response.topics.push(topic_response);
        let resp_msg = ResponseMessage::new(10, response);

        debug!(
            "res msg write size: {}",
            resp_msg.write_size(KfFileFetchRequest::DEFAULT_API_VERSION)
        );

        socket
            .get_mut_sink()
            .encode_file_slices(&resp_msg, KfFileFetchRequest::DEFAULT_API_VERSION)
            .await?;
        debug!("server: finish sending out");
        Ok(())
    }

    async fn setup_client(addr: SocketAddr) -> Result<(), KfSocketError> {
        sleep(Duration::from_millis(50)).await;
        debug!("client: trying to connect");
        let mut socket = KfSocket::connect(&addr).await?;
        debug!("client: connect to test server and waiting...");

        let req_msg: RequestMessage<DefaultKfFetchRequest> = RequestMessage::default();
        let res_msg = socket.send(&req_msg).await?;

        debug!("output: {:#?}", res_msg);
        let topic_responses = res_msg.response.topics;
        assert_eq!(topic_responses.len(), 1);
        let part_responses = &topic_responses[0].partitions;
        assert_eq!(part_responses.len(), 1);
        let batches = &part_responses[0].records.batches;
        assert_eq!(batches.len(), 1);
        let records = &batches[0].records;
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].value.to_string(), "record 0");
        assert_eq!(records[1].value.to_string(), "record 1");

        Ok(())
    }

    #[test_async]
    async fn test_save_fetch() -> Result<(), KfSocketError> {
        // create fetch and save
        setup_batch_file().await?;

        let addr = "127.0.0.1:9911".parse::<SocketAddr>().expect("parse");

        let _r = join(setup_client(addr), test_server(addr.clone())).await;

        Ok(())
    }
}

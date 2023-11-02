use std::fmt::Debug;
use std::io::{Error, ErrorKind};
use std::marker::PhantomData;
use std::time::Duration;
use bytes::{Buf, BufMut};

use fluvio_protocol::record::RawRecords;
use fluvio_protocol::Encoder;
use fluvio_protocol::Decoder;
use fluvio_protocol::derive::FluvioDefault;
use fluvio_protocol::Version;
use fluvio_protocol::api::Request;
use fluvio_protocol::record::RecordSet;
use fluvio_types::PartitionId;

use crate::COMMON_VERSION;
use crate::isolation::Isolation;

use super::ProduceResponse;
use crate::server::smartmodule::SmartModuleInvocation;

pub type DefaultProduceRequest = ProduceRequest<RecordSet<RawRecords>>;
pub type DefaultPartitionRequest = PartitionProduceData<RecordSet<RawRecords>>;
pub type DefaultTopicRequest = TopicProduceData<RecordSet<RawRecords>>;

const PRODUCER_TRANSFORMATION_API_VERSION: i16 = 8;

#[derive(FluvioDefault, Debug)]
pub struct ProduceRequest<R> {
    /// The transactional ID, or null if the producer is not transactional.
    #[fluvio(min_version = 3)]
    pub transactional_id: Option<String>,

    /// ReadUncommitted - Just wait for leader to write message (only wait for LEO update).
    /// ReadCommitted - Wait for messages to be committed (wait for HW).
    pub isolation: Isolation,

    /// The timeout to await a response.
    pub timeout: Duration,

    /// Each topic to produce to.
    pub topics: Vec<TopicProduceData<R>>,

    #[fluvio(min_version = 8)]
    pub smartmodules: Vec<SmartModuleInvocation>,

    pub data: PhantomData<R>,
}

impl<R> Request for ProduceRequest<R>
where
    R: Debug + Decoder + Encoder,
{
    const API_KEY: u16 = 0;

    const MIN_API_VERSION: i16 = 0;
    const DEFAULT_API_VERSION: i16 = COMMON_VERSION;

    type Response = ProduceResponse;
}

#[derive(Encoder, Decoder, FluvioDefault, Debug)]
pub struct TopicProduceData<R> {
    /// The topic name.
    pub name: String,

    /// Each partition to produce to.
    pub partitions: Vec<PartitionProduceData<R>>,
    pub data: PhantomData<R>,
}

#[derive(Encoder, Decoder, FluvioDefault, Debug)]
pub struct PartitionProduceData<R> {
    /// The partition index.
    pub partition_index: PartitionId,

    /// The record data to be produced.
    pub records: R,
}

impl<R> Encoder for ProduceRequest<R>
where
    R: Encoder + Decoder + Default + Debug,
{
    fn write_size(&self, version: Version) -> usize {
        self.transactional_id.write_size(version)
            + IsolationData(0i16).write_size(version)
            + TimeoutData(0i32).write_size(version)
            + self.topics.write_size(version)
            + if version >= PRODUCER_TRANSFORMATION_API_VERSION {
                self.smartmodules.write_size(version)
            } else {
                0
            }
    }

    fn encode<T>(&self, dest: &mut T, version: Version) -> Result<(), Error>
    where
        T: BufMut,
    {
        self.transactional_id.encode(dest, version)?;
        IsolationData::from(self.isolation).encode(dest, version)?;
        TimeoutData::try_from(self.timeout)?.encode(dest, version)?;
        self.topics.encode(dest, version)?;
        if version >= PRODUCER_TRANSFORMATION_API_VERSION {
            self.smartmodules.encode(dest, version)?;
        }
        Ok(())
    }
}

impl<R> Decoder for ProduceRequest<R>
where
    R: Decoder + Encoder + Default + Debug,
{
    fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        self.transactional_id = Decoder::decode_from(src, version)?;
        self.isolation = Isolation::from(IsolationData::decode_from(src, version)?);
        self.timeout = Duration::try_from(TimeoutData::decode_from(src, version)?)?;
        self.topics = Decoder::decode_from(src, version)?;
        if version >= PRODUCER_TRANSFORMATION_API_VERSION {
            self.smartmodules.decode(src, version)?;
        }
        Ok(())
    }
}

impl<R: Encoder + Decoder + Default + Debug + Clone> Clone for ProduceRequest<R> {
    fn clone(&self) -> Self {
        Self {
            transactional_id: self.transactional_id.clone(),
            isolation: self.isolation,
            timeout: self.timeout,
            topics: self.topics.clone(),
            data: self.data,
            smartmodules: self.smartmodules.clone(),
        }
    }
}

impl<R: Encoder + Decoder + Default + Debug + Clone> Clone for TopicProduceData<R> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            partitions: self.partitions.clone(),
            data: self.data,
        }
    }
}

impl<R: Encoder + Decoder + Default + Debug + Clone> Clone for PartitionProduceData<R> {
    fn clone(&self) -> Self {
        Self {
            partition_index: self.partition_index,
            records: self.records.clone(),
        }
    }
}

/// Isolation is represented in binary format as i16 value (field `acks` in Kafka wire protocol).
#[derive(Encoder, Decoder, FluvioDefault, Debug)]
struct IsolationData(i16);

impl From<Isolation> for IsolationData {
    fn from(isolation: Isolation) -> Self {
        IsolationData(match isolation {
            Isolation::ReadUncommitted => 1,
            Isolation::ReadCommitted => -1,
        })
    }
}

impl From<IsolationData> for Isolation {
    fn from(data: IsolationData) -> Self {
        match data.0 {
            acks if acks < 0 => Isolation::ReadCommitted,
            _ => Isolation::ReadUncommitted,
        }
    }
}

/// Timeout duration is represented in binary format as i32 value (field `timeout_ms` in Kafka wire protocol).
#[derive(Encoder, Decoder, FluvioDefault, Debug)]
struct TimeoutData(i32);

impl TryFrom<Duration> for TimeoutData {
    type Error = Error;

    fn try_from(value: Duration) -> Result<Self, Self::Error> {
        value.as_millis().try_into().map(TimeoutData).map_err(|_e| {
            Error::new(
                ErrorKind::InvalidInput,
                "Timeout must fit into 4 bytes integer value",
            )
        })
    }
}

impl TryFrom<TimeoutData> for Duration {
    type Error = Error;

    fn try_from(value: TimeoutData) -> Result<Self, Self::Error> {
        u64::try_from(value.0)
            .map(Duration::from_millis)
            .map_err(|_e| {
                Error::new(
                    ErrorKind::InvalidInput,
                    "Timeout must be positive integer value",
                )
            })
    }
}

#[cfg(feature = "file")]
pub use file::*;

#[cfg(feature = "file")]
mod file {
    use std::io::Error as IoError;

    use tracing::trace;
    use bytes::BytesMut;

    use fluvio_protocol::Version;
    use fluvio_protocol::store::FileWrite;
    use fluvio_protocol::store::StoreValue;

    use crate::file::FileRecordSet;

    use super::*;

    pub type FileProduceRequest = ProduceRequest<FileRecordSet>;
    pub type FileTopicRequest = TopicProduceData<FileRecordSet>;
    pub type FilePartitionRequest = PartitionProduceData<FileRecordSet>;

    impl FileWrite for FileProduceRequest {
        fn file_encode(
            &self,
            src: &mut BytesMut,
            data: &mut Vec<StoreValue>,
            version: Version,
        ) -> Result<(), IoError> {
            trace!("file encoding produce request");
            self.transactional_id.encode(src, version)?;
            IsolationData::from(self.isolation).encode(src, version)?;
            TimeoutData::try_from(self.timeout)?.encode(src, version)?;
            self.topics.file_encode(src, data, version)?;
            Ok(())
        }
    }

    impl FileWrite for FileTopicRequest {
        fn file_encode(
            &self,
            src: &mut BytesMut,
            data: &mut Vec<StoreValue>,
            version: Version,
        ) -> Result<(), IoError> {
            trace!("file encoding produce topic request");
            self.name.encode(src, version)?;
            self.partitions.file_encode(src, data, version)?;
            Ok(())
        }
    }

    impl FileWrite for FilePartitionRequest {
        fn file_encode(
            &self,
            src: &mut BytesMut,
            data: &mut Vec<StoreValue>,
            version: Version,
        ) -> Result<(), IoError> {
            trace!("file encoding for partition request");
            self.partition_index.encode(src, version)?;
            self.records.file_encode(src, data, version)?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Error, ErrorKind};
    use std::time::Duration;

    use fluvio_protocol::{Decoder, Encoder};
    use fluvio_protocol::api::Request;
    use fluvio_protocol::record::Batch;
    use fluvio_protocol::record::{Record, RecordData, RecordSet};
    use fluvio_smartmodule::dataplane::smartmodule::{SmartModuleExtraParams, Lookback};

    use crate::produce::DefaultProduceRequest;
    use crate::produce::TopicProduceData;
    use crate::produce::PartitionProduceData;
    use crate::isolation::Isolation;
    use crate::produce::request::PRODUCER_TRANSFORMATION_API_VERSION;
    use crate::server::smartmodule::{
        SmartModuleInvocation, SmartModuleInvocationWasm, SmartModuleKind,
    };

    #[test]
    fn test_encode_decode_produce_request_isolation_timeout() -> Result<(), Error> {
        let request = DefaultProduceRequest {
            isolation: Isolation::ReadCommitted,
            timeout: Duration::from_millis(123456),
            ..Default::default()
        };

        let version = DefaultProduceRequest::DEFAULT_API_VERSION;
        let mut bytes = request.as_bytes(version)?;

        let decoded: DefaultProduceRequest = Decoder::decode_from(&mut bytes, version)?;

        assert_eq!(request.isolation, decoded.isolation);
        assert_eq!(request.timeout, decoded.timeout);
        Ok(())
    }

    #[test]
    fn test_encode_produce_request_timeout_too_big() {
        let request = DefaultProduceRequest {
            isolation: Isolation::ReadCommitted,
            timeout: Duration::from_millis(u64::MAX),
            ..Default::default()
        };

        let version = DefaultProduceRequest::DEFAULT_API_VERSION;
        let result = request.as_bytes(version).expect_err("expected error");

        assert_eq!(result.kind(), ErrorKind::InvalidInput);
        assert_eq!(
            result.to_string(),
            "Timeout must fit into 4 bytes integer value"
        );
    }

    #[test]
    fn test_default_produce_request_clone() {
        //given
        let request = DefaultProduceRequest {
            transactional_id: Some("transaction_id".to_string()),
            isolation: Default::default(),
            timeout: Duration::from_millis(100),
            topics: vec![TopicProduceData {
                name: "topic".to_string(),
                partitions: vec![PartitionProduceData {
                    partition_index: 1,
                    records: RecordSet {
                        batches: vec![Batch::from(vec![Record::new(RecordData::from(
                            "some raw data",
                        ))])
                        .try_into()
                        .expect("compressed batch")],
                    },
                }],
                data: Default::default(),
            }],
            data: Default::default(),
            smartmodules: Default::default(),
        };
        let version = DefaultProduceRequest::DEFAULT_API_VERSION;

        //when
        #[allow(clippy::redundant_clone)]
        let cloned = request.clone();
        let bytes = request.as_bytes(version).expect("encoded request");
        let cloned_bytes = cloned.as_bytes(version).expect("encoded cloned request");

        //then
        assert_eq!(bytes, cloned_bytes);
    }

    #[test]
    fn test_encode_produce_request() {
        //given
        let mut dest = Vec::new();
        let params = SmartModuleExtraParams::default();
        let value = DefaultProduceRequest {
            transactional_id: Some("t_id".into()),
            isolation: Isolation::ReadCommitted,
            timeout: Duration::from_secs(1),
            topics: vec![],
            smartmodules: vec![SmartModuleInvocation {
                wasm: SmartModuleInvocationWasm::AdHoc(vec![0xde, 0xad, 0xbe, 0xef]),
                kind: SmartModuleKind::Filter,
                params,
            }],
            data: std::marker::PhantomData,
        };
        //when
        value
            .encode(&mut dest, PRODUCER_TRANSFORMATION_API_VERSION)
            .expect("should encode");

        //then
        let expected = vec![
            0x01, 0x00, 0x04, 0x74, 0x5f, 0x69, 0x64, 0xff, 0xff, 0x00, 0x00, 0x03, 0xe8, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x04, 0xde, 0xad,
            0xbe, 0xef, 0x00, 0x00, 0x00,
        ];
        assert_eq!(dest, expected);
    }

    #[test]
    fn test_decode_produce_request() {
        //given
        let bytes = vec![
            0x01, 0x00, 0x04, 0x74, 0x5f, 0x69, 0x64, 0xff, 0xff, 0x00, 0x00, 0x03, 0xe8, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x04, 0xde, 0xad,
            0xbe, 0xef, 0x00, 0x00, 0x00,
        ];
        let mut value = DefaultProduceRequest::default();

        //when
        value
            .decode(
                &mut std::io::Cursor::new(bytes),
                PRODUCER_TRANSFORMATION_API_VERSION,
            )
            .unwrap();

        //then
        assert_eq!(value.transactional_id, Some("t_id".into()));
        assert_eq!(value.isolation, Isolation::ReadCommitted);
        assert_eq!(value.timeout, Duration::from_secs(1));
        assert!(value.topics.is_empty());
        let sm = match value.smartmodules.first() {
            Some(wasm) => wasm,
            _ => panic!("should have smartstreeam payload"),
        };
        assert!(sm.params.lookback().is_none());
        let wasm = match &sm.wasm {
            SmartModuleInvocationWasm::AdHoc(wasm) => wasm.as_slice(),
            #[allow(unreachable_patterns)]
            _ => panic!("should be SmartModuleInvocationWasm::AdHoc"),
        };
        assert_eq!(wasm, vec![0xde, 0xad, 0xbe, 0xef]);
        assert!(matches!(sm.kind, SmartModuleKind::Filter));
    }

    #[test]
    fn test_encode_produce_request_last_version() {
        //given
        let mut dest = Vec::new();
        let mut params = SmartModuleExtraParams::default();
        params.set_lookback(Some(Lookback::last(1)));
        let value = DefaultProduceRequest {
            transactional_id: Some("t_id".into()),
            isolation: Isolation::ReadCommitted,
            timeout: Duration::from_secs(1),
            topics: vec![],
            smartmodules: vec![SmartModuleInvocation {
                wasm: SmartModuleInvocationWasm::AdHoc(vec![0xde, 0xad, 0xbe, 0xef]),
                kind: SmartModuleKind::Filter,
                params,
            }],
            data: std::marker::PhantomData,
        };
        //when
        value
            .encode(&mut dest, DefaultProduceRequest::MAX_API_VERSION)
            .expect("should encode");

        //then
        let expected = vec![
            0x01, 0x00, 0x04, 0x74, 0x5f, 0x69, 0x64, 0xff, 0xff, 0x00, 0x00, 0x03, 0xe8, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x04, 0xde, 0xad,
            0xbe, 0xef, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
            0x00,
        ];
        assert_eq!(dest, expected);
    }

    #[test]
    fn test_encode_produce_request_prev_version() {
        //given
        let mut dest = Vec::new();
        let mut params = SmartModuleExtraParams::default();
        params.set_lookback(Some(Lookback::age(Duration::from_secs(20), Some(1))));
        let value = DefaultProduceRequest {
            transactional_id: Some("t_id".into()),
            isolation: Isolation::ReadCommitted,
            timeout: Duration::from_secs(1),
            topics: vec![],
            smartmodules: vec![SmartModuleInvocation {
                wasm: SmartModuleInvocationWasm::AdHoc(vec![0xde, 0xad, 0xbe, 0xef]),
                kind: SmartModuleKind::Filter,
                params,
            }],
            data: std::marker::PhantomData,
        };
        //when
        value
            .encode(&mut dest, DefaultProduceRequest::MAX_API_VERSION - 1)
            .expect("should encode");

        //then
        let expected = vec![
            0x01, 0x00, 0x04, 0x74, 0x5f, 0x69, 0x64, 0xff, 0xff, 0x00, 0x00, 0x03, 0xe8, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x04, 0xde, 0xad,
            0xbe, 0xef, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00,
        ];
        assert_eq!(dest, expected);
    }

    #[test]
    fn test_decode_produce_request_last_version() {
        //given
        let bytes = vec![
            0x01, 0x00, 0x04, 0x74, 0x5f, 0x69, 0x64, 0xff, 0xff, 0x00, 0x00, 0x03, 0xe8, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x04, 0xde, 0xad,
            0xbe, 0xef, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
            0x00,
        ];
        let mut value = DefaultProduceRequest::default();

        //when
        value
            .decode(
                &mut std::io::Cursor::new(bytes),
                DefaultProduceRequest::MAX_API_VERSION,
            )
            .unwrap();

        //then
        assert_eq!(value.transactional_id, Some("t_id".into()));
        assert_eq!(value.isolation, Isolation::ReadCommitted);
        assert_eq!(value.timeout, Duration::from_secs(1));
        assert!(value.topics.is_empty());
        let sm = match value.smartmodules.first() {
            Some(wasm) => wasm,
            _ => panic!("should have smartstreeam payload"),
        };
        assert_eq!(sm.params.lookback(), Some(&Lookback::last(1)));
        let wasm = match &sm.wasm {
            SmartModuleInvocationWasm::AdHoc(wasm) => wasm.as_slice(),
            #[allow(unreachable_patterns)]
            _ => panic!("should be SmartModuleInvocationWasm::AdHoc"),
        };
        assert_eq!(wasm, vec![0xde, 0xad, 0xbe, 0xef]);
        assert!(matches!(sm.kind, SmartModuleKind::Filter));
    }

    #[test]
    fn test_decode_produce_request_prev_version() {
        //given
        let bytes = vec![
            0x01, 0x00, 0x04, 0x74, 0x5f, 0x69, 0x64, 0xff, 0xff, 0x00, 0x00, 0x03, 0xe8, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x04, 0xde, 0xad,
            0xbe, 0xef, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
            0x00,
        ];
        let mut value = DefaultProduceRequest::default();

        //when
        value
            .decode(
                &mut std::io::Cursor::new(bytes),
                DefaultProduceRequest::MAX_API_VERSION - 1,
            )
            .unwrap();

        //then
        assert_eq!(value.transactional_id, Some("t_id".into()));
        assert_eq!(value.isolation, Isolation::ReadCommitted);
        assert_eq!(value.timeout, Duration::from_secs(1));
        assert!(value.topics.is_empty());
        let sm = match value.smartmodules.first() {
            Some(wasm) => wasm,
            _ => panic!("should have smartstreeam payload"),
        };
        assert_eq!(sm.params.lookback(), Some(&Lookback::last(1)));
        let wasm = match &sm.wasm {
            SmartModuleInvocationWasm::AdHoc(wasm) => wasm.as_slice(),
            #[allow(unreachable_patterns)]
            _ => panic!("should be SmartModuleInvocationWasm::AdHoc"),
        };
        assert_eq!(wasm, vec![0xde, 0xad, 0xbe, 0xef]);
        assert!(matches!(sm.kind, SmartModuleKind::Filter));
    }
}

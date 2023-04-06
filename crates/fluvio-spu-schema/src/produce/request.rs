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

    #[fluvio(min_version = PRODUCER_TRANSFORMATION_API)]
    pub smartmodules: Vec<SmartModuleInvocation>,

    pub data: PhantomData<R>,
}

impl<R> Request for ProduceRequest<R>
where
    R: Debug + Decoder + Encoder,
{
    const API_KEY: u16 = 0;

    const MIN_API_VERSION: i16 = 0;
    const MAX_API_VERSION: i16 = PRODUCER_TRANSFORMATION_API_VERSION;
    const DEFAULT_API_VERSION: i16 = PRODUCER_TRANSFORMATION_API_VERSION;

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

    use crate::produce::DefaultProduceRequest;
    use crate::produce::TopicProduceData;
    use crate::produce::PartitionProduceData;
    use crate::isolation::Isolation;

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
        let cloned = request.clone();
        let bytes = request.as_bytes(version).expect("encoded request");
        let cloned_bytes = cloned.as_bytes(version).expect("encoded cloned request");

        //then
        assert_eq!(bytes, cloned_bytes);
    }
}

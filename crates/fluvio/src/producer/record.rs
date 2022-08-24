use std::sync::Arc;

use async_channel::Receiver;
use async_lock::RwLock;

use fluvio_protocol::record::Offset;
use fluvio_protocol::link::ErrorCode;
use fluvio_types::PartitionId;

use crate::error::Result;
use crate::producer::accumulator::ProducePartitionResponseFuture;

use super::error::ProducerError;

/// Metadata of a record send to a topic
#[derive(Clone, Debug, Default)]
pub struct RecordMetadata {
    /// The partition the record was sent to
    pub(crate) partition_id: PartitionId,
    /// The offset of the record in the topic/partition.
    pub(crate) offset: Offset,
}

impl RecordMetadata {
    /// The offset of the record in the topic/partition.
    pub fn offset(&self) -> Offset {
        self.offset
    }

    /// Partition index the record was sent to
    pub fn partition_id(&self) -> PartitionId {
        self.partition_id
    }
}

/// Possible states of a batch in the accumulator
pub(crate) enum BatchMetadataState {
    /// The batch is buffered and ready to be sent to the SPU
    Buffered(Receiver<ProducePartitionResponseFuture>),
    /// The batch was sent to the SPU. Base offset is known
    Sent(Offset),
    /// There was an error sending the batch to the SPU
    Failed(ProducerError),
}

pub(crate) struct BatchMetadata {
    state: RwLock<BatchMetadataState>,
}

impl BatchMetadata {
    pub(crate) fn new(receiver: Receiver<ProducePartitionResponseFuture>) -> Self {
        Self {
            state: RwLock::new(BatchMetadataState::Buffered(receiver)),
        }
    }

    /// Wait for the base offset of the batch. This is the offset of the first
    /// record in the batch and it is known once the batch is sent to the server.
    pub(crate) async fn base_offset(&self) -> Result<Offset> {
        let mut state = self.state.write().await;
        match &*state {
            BatchMetadataState::Buffered(receiver) => {
                let msg = receiver
                    .recv()
                    .await
                    .map_err(|err| ProducerError::GetRecordMetadata(Some(err)));

                match msg {
                    Ok(offset_future) => {
                        let (offset, error) = offset_future.await;
                        if error == ErrorCode::None {
                            *state = BatchMetadataState::Sent(offset);
                            Ok(offset)
                        } else {
                            let error = ProducerError::SpuErrorCode(error);
                            *state = BatchMetadataState::Failed(error.clone());
                            Err(error.into())
                        }
                    }
                    Err(err) => {
                        *state = BatchMetadataState::Failed(err.clone());
                        Err(err.into())
                    }
                }
            }
            BatchMetadataState::Sent(offset) => Ok(*offset),
            BatchMetadataState::Failed(error) => Err(error.clone().into()),
        }
    }
}

/// Partial information about record metadata.
/// Used to create FutureRecordMetadata once we have the partition id.
pub(crate) struct PartialFutureRecordMetadata {
    /// The offset of the record in the topic/partition.
    relative_offset: Offset,
    batch_metadata: Arc<BatchMetadata>,
}

impl PartialFutureRecordMetadata {
    pub(crate) fn new(relative_offset: Offset, batch_metadata: Arc<BatchMetadata>) -> Self {
        Self {
            relative_offset,
            batch_metadata,
        }
    }

    pub(crate) fn into_future_record_metadata(
        self,
        partition_id: PartitionId,
    ) -> FutureRecordMetadata {
        FutureRecordMetadata {
            partition_id,
            relative_offset: self.relative_offset,
            batch_metadata: self.batch_metadata,
        }
    }
}

/// Output of `TopicProducer::send`
/// Used to wait the `RecordMetadata` of the record being sent.
/// See `FutureRecordMetadata::wait`
pub struct FutureRecordMetadata {
    /// The partition the record was sent to
    pub(crate) partition_id: PartitionId,
    /// The offset of the record in the topic/partition.
    pub(crate) relative_offset: Offset,
    /// Handler to get base offset of the batch
    pub(crate) batch_metadata: Arc<BatchMetadata>,
}

impl FutureRecordMetadata {
    /// wait for the record metadata to be available
    pub async fn wait(self) -> Result<RecordMetadata> {
        let base_offset = self.batch_metadata.base_offset().await?;
        Ok(RecordMetadata {
            partition_id: self.partition_id,
            offset: base_offset + self.relative_offset,
        })
    }
}

use crate::producer::record::{RecordMetadata, FutureRecordMetadata};
use crate::producer::error::ProducerError;
use crate::error::Result;

/// Struct returned by of TopicProduce::send call, it is used
///  to gather the record metadata associated to each send call.
#[derive(Default)]
pub struct ProduceOutput {
    record_metadata: Vec<FutureRecordMetadata>,
}

impl ProduceOutput {
    /// Add future record metadata to the output.
    pub(crate) fn add(&mut self, record_metadata: FutureRecordMetadata) {
        self.record_metadata.push(record_metadata);
    }

    /// Wait for all record metadata of all records sent using smartmodule
    #[cfg(feature = "smartengine")]
    pub async fn wait_all(self) -> Result<Vec<RecordMetadata>> {
        let mut records_metadata = Vec::with_capacity(self.record_metadata.len());

        for future in self.record_metadata.into_iter() {
            let metadata = future.wait().await?;
            records_metadata.push(metadata)
        }
        Ok(records_metadata)
    }

    /// Wait for the record metadata
    pub async fn wait(self) -> Result<RecordMetadata> {
        let future_record_metadata = self
            .record_metadata
            .into_iter()
            .next()
            .ok_or(ProducerError::GetRecordMetadata(None))?;
        let record_metadata = future_record_metadata.wait().await?;
        Ok(record_metadata)
    }
}

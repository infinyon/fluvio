use std::{
    sync::atomic::{AtomicU64, Ordering},
    ops::AddAssign,
};

use fluvio_protocol::record::Batch;
use fluvio_smartengine::metrics::SmartModuleChainMetrics;
use fluvio_spu_schema::fetch::FilePartitionResponse;
use serde::Serialize;

#[derive(Default, Debug, Serialize)]
pub(crate) struct SpuMetrics {
    inbound: Activity,
    outbound: Activity,
    smartmodule: SmartModuleChainMetrics,
}

impl SpuMetrics {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub fn inbound(&self) -> &Activity {
        &self.inbound
    }

    pub fn outbound(&self) -> &Activity {
        &self.outbound
    }

    pub fn chain_metrics(&self) -> &SmartModuleChainMetrics {
        &self.smartmodule
    }
}

#[derive(Default, Debug, Serialize)]
pub(crate) struct Record {
    records: AtomicU64,
    bytes: AtomicU64,
}

impl Record {
    pub(crate) fn increase(&self, records: u64, bytes: u64) {
        self.records.fetch_add(records, Ordering::SeqCst);
        self.bytes.fetch_add(bytes, Ordering::SeqCst);
    }
}

#[derive(Default, Debug, Serialize)]
pub struct Activity {
    connector: Record,
    client: Record,
}

#[derive(Default, Debug)]
pub(crate) struct IncreaseValue {
    records: u64,
    bytes: u64,
}

impl Activity {
    pub(crate) fn increase(&self, connector: bool, records: u64, bytes: u64) {
        if connector {
            self.connector.increase(records, bytes);
        } else {
            self.client.increase(records, bytes);
        }
    }

    pub(crate) fn increase_by_value(&self, connector: bool, value: IncreaseValue) {
        let IncreaseValue { records, bytes } = value;
        self.increase(connector, records, bytes)
    }
}

#[cfg(test)]
impl Activity {
    pub fn connector_records(&self) -> u64 {
        self.connector.records.load(Ordering::SeqCst)
    }
    pub fn connector_bytes(&self) -> u64 {
        self.connector.bytes.load(Ordering::SeqCst)
    }
    pub fn client_records(&self) -> u64 {
        self.client.records.load(Ordering::SeqCst)
    }
    pub fn client_bytes(&self) -> u64 {
        self.client.bytes.load(Ordering::SeqCst)
    }
}

impl IncreaseValue {
    pub(crate) fn new(records: u64, bytes: u64) -> Self {
        Self { records, bytes }
    }
}

// Measuring of serialized data. `bytes` is length of file slice, `records` is an offset's change
impl From<&FilePartitionResponse> for IncreaseValue {
    fn from(resp: &FilePartitionResponse) -> Self {
        Self::new(
            (resp.high_watermark - resp.log_start_offset) as u64,
            resp.records.len() as u64,
        )
    }
}

// Measuring of deserialized data. `bytes` is a sum of key size and value size of every record, `records` is records size
impl From<&Batch> for IncreaseValue {
    fn from(batch: &Batch) -> Self {
        let records = batch.records().len();
        let mut bytes = 0;
        for record in batch.records() {
            bytes.add_assign(record.key().map_or(0, |k| k.len()));
            bytes.add_assign(record.value.len());
        }
        Self::new(records as u64, bytes as u64)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use fluvio::RecordKey;
    use fluvio_future::file_slice::AsyncFileSlice;
    use fluvio_protocol::record::Record;
    use fluvio_protocol::record::RecordData;
    use fluvio_spu_schema::file::FileRecordSet;

    #[test]
    fn test_increase_from_batch() {
        //given
        let record1_value = [1; 10];
        let record2_value = [2; 11];
        let record2_key = [3; 12];
        let batch = Batch::from(vec![
            Record::new(RecordData::from(record1_value)),
            Record::new_key_value(
                RecordKey::from(record2_key),
                RecordData::from(record2_value),
            ),
        ]);
        let activity = Activity::default();

        //when
        activity.increase_by_value(true, IncreaseValue::from(&batch));

        //then
        assert_eq!(activity.connector.records.load(Ordering::SeqCst), 2);
        assert_eq!(activity.connector.bytes.load(Ordering::SeqCst), 33); // 10 + 11 + 12
    }

    #[test]
    fn test_increase_from_file_partition_response() {
        //given
        let response = FilePartitionResponse {
            high_watermark: 2,
            log_start_offset: 1,
            records: FileRecordSet::from(AsyncFileSlice::new(Default::default(), 0, 123)),
            ..Default::default()
        };
        let activity = Activity::default();

        //when
        activity.increase_by_value(true, IncreaseValue::from(&response));

        //then
        assert_eq!(activity.connector.records.load(Ordering::SeqCst), 1);
        assert_eq!(activity.connector.bytes.load(Ordering::SeqCst), 123);
    }
}

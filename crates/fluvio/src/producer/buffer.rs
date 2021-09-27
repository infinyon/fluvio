use std::collections::VecDeque;
use fluvio_protocol::Encoder;
use dataplane::produce::ProduceRequest;
use dataplane::record::{RecordSet, Record};
use fluvio_protocol::api::Request;

use crate::producer::assoc::AssociatedRecord;

/// A buffer for storing records given to us by the producer's caller.
///
/// This buffer has a max_size capacity (in bytes) after which it will no
/// longer accept new records. This is intended to notify the dispatcher
/// when it must perform a flush.
#[derive(Debug)]
pub(crate) struct RecordBuffer {
    size: usize,
    max_size: usize,
    records: VecDeque<AssociatedRecord>,
}

impl RecordBuffer {
    /// Create a `RecordBuffer` with the given max_size (in bytes).
    ///
    /// This buffer will accept new records until the size limit is exceeded.
    pub fn new(max_size: usize) -> Self {
        Self {
            size: 0,
            max_size,
            records: VecDeque::new(),
        }
    }

    /// Add a new [`Record`] to the buffer. If this record would cause the buffer to
    /// exceed the `max_size`, the record is rejected and returned in an `Err`.
    pub fn push(&mut self, pending: AssociatedRecord) -> Result<(), AssociatedRecord> {
        let record_size = Encoder::write_size(
            &pending.record,
            ProduceRequest::<RecordSet>::DEFAULT_API_VERSION,
        );

        if self.size + record_size > self.max_size {
            return Err(pending);
        }

        self.records.push_back(pending);
        self.size += record_size;

        Ok(())
    }

    /// The `RecordBuffer` acts as a queue, and `pop` removes the next queued record.
    ///
    /// Records `push`ed into the `RecordBuffer` will be `pop`ped in the same order.
    pub fn pop(&mut self) -> Option<AssociatedRecord> {
        let pending = self.records.pop_front();
        match pending {
            None => None,
            Some(pending) => {
                let record_size = Encoder::write_size(
                    &pending.record,
                    ProduceRequest::<RecordSet>::DEFAULT_API_VERSION,
                );
                self.size = self.size.saturating_sub(record_size);
                Some(pending)
            }
        }
    }

    /// Returns the current accumulated size (in bytes) of the Records in this buffer.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Returns the current length (number of records) of this buffer.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Returns true if the given record is smaller than the max size of this buffer.
    ///
    /// Basically, we may need to flush to make room for this record, but could it
    /// even possibly fit?
    pub fn could_fit(&self, record: &AssociatedRecord) -> bool {
        let record_size = Encoder::write_size(
            &record.record,
            ProduceRequest::<RecordSet>::DEFAULT_API_VERSION,
        );
        record_size < self.max_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dataplane::record::RecordKey;
    use dataplane::ReplicaKey;
    use crate::producer::RecordUid;

    /// Each of the records in this test is 8 bytes.
    /// The buffer is initialized with a capacity of 20 bytes.
    /// After receiving two records, the buffer should start rejecting records.
    #[test]
    fn test_buffer_happy() {
        let mut buffer = RecordBuffer::new(20);

        // Pushing the first (8 byte) record should succeed
        let record = Record::from((RecordKey::NULL, "A"));
        let assoc_record = AssociatedRecord {
            uid: RecordUid(0),
            replica_key: ReplicaKey::new("One", 0),
            record: record.clone(),
        };
        assert!(buffer.could_fit(&assoc_record));
        buffer.push(assoc_record).expect("first record should fit");
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.size(), 8);

        // Pushing the second (8 byte) record should succeed
        let record = Record::from((RecordKey::NULL, "B"));
        let assoc_record = AssociatedRecord {
            uid: RecordUid(1),
            replica_key: ReplicaKey::new("One", 0),
            record: record.clone(),
        };
        buffer.push(assoc_record).expect("second record should fit");
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer.size(), 16);

        // Pushing the third (8 byte) record should fail, and the record will be "bounced" back.
        let record = Record::from((RecordKey::NULL, "C"));
        let assoc_record = AssociatedRecord {
            uid: RecordUid(2),
            replica_key: ReplicaKey::new("One", 0),
            record: record.clone(),
        };
        let bounced = buffer
            .push(assoc_record)
            .expect_err("third record should NOT fit");
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer.size(), 16);
        assert_eq!(bounced.uid, RecordUid(2));
        assert_eq!(bounced.replica_key, ReplicaKey::new("One", 0));
        assert_eq!(bounced.record.key, record.key);
        assert_eq!(bounced.record.value, record.value);

        // Check that popping the first record works
        let assoc_record = buffer.pop().expect("should pop first record");
        assert_eq!(assoc_record.uid, RecordUid(0));
        assert_eq!(assoc_record.replica_key, ReplicaKey::new("One", 0));
        assert_eq!(assoc_record.record.key, None);
        assert_eq!(assoc_record.record.value, "A".into());
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.size(), 8);

        // Check that we can now push the next record
        buffer
            .push(bounced)
            .expect("third record should fit when there's room");
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer.size(), 16);

        // Check that popping the second record works
        let popped = buffer.pop().expect("should pop first record");
        assert_eq!(popped.uid, RecordUid(1));
        assert_eq!(popped.replica_key, ReplicaKey::new("One", 0));
        assert_eq!(popped.record.key, None);
        assert_eq!(popped.record.value, "B".into());
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.size(), 8);

        // Check that popping the third record works
        let popped = buffer.pop().expect("should pop first record");
        assert_eq!(popped.uid, RecordUid(2));
        assert_eq!(popped.replica_key, ReplicaKey::new("One", 0));
        assert_eq!(popped.record.key, None);
        assert_eq!(popped.record.value, "C".into());
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.size(), 0);

        // Check that popping the empty buffer gives nothing
        assert!(buffer.pop().is_none());
    }

    #[test]
    fn test_buffer_no_fit() {
        let buffer = RecordBuffer::new(20);
        let big_record = Record::from((RecordKey::NULL, "ABCDEFGHIJKLMNOPQRSTUVWXYZ"));
        let assoc_record = AssociatedRecord {
            uid: RecordUid(0),
            replica_key: ReplicaKey::new("One", 0),
            record: big_record,
        };

        // The record above could NEVER fit into this buffer, even when it is empty.
        assert!(!buffer.could_fit(&assoc_record));
    }
}

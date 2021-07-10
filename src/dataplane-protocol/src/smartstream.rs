use fluvio_protocol::derive::{Encode, Decode};

/// Used to pass both the accumulator and records to an Aggregate smartstream
#[derive(Debug, Default, Encode, Decode)]
pub struct Aggregate {
    pub accumulator: Vec<u8>,
    pub records: Vec<u8>,
}

use fluvio_protocol::{Decoder, Encoder};

#[derive(Debug, Default, Encoder, Decoder, Clone)]
pub struct AddPartition {
    pub number_of_partition: u32,
}

#[derive(Debug, Encoder, Decoder, Clone)]
pub enum UpdateTopicAction {
    #[fluvio(tag = 0)]
    AddPartition(AddPartition),
}

impl Default for UpdateTopicAction {
    fn default() -> Self {
        Self::AddPartition(AddPartition::default())
    }
}

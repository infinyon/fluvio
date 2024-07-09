use fluvio_protocol::{Decoder, Encoder};

#[derive(Debug, Default, Encoder, Decoder, Clone)]
pub struct AddPartition {
    pub count: u32,
}

#[derive(Debug, Default, Encoder, Decoder, Clone)]
pub struct AddMirror {
    pub remote_cluster: String,
}

#[derive(Debug, Encoder, Decoder, Clone)]
pub enum UpdateTopicAction {
    #[fluvio(tag = 0)]
    AddPartition(AddPartition),
    #[fluvio(tag = 1)]
    AddMirror(AddMirror),
}

impl Default for UpdateTopicAction {
    fn default() -> Self {
        Self::AddPartition(AddPartition::default())
    }
}

use std::fmt::Debug;

use fluvio_protocol::{Encoder, Decoder};
use fluvio_controlplane_metadata::message::Message;

/// General control plane request
#[derive(Decoder, Encoder, Debug, Default)]
pub struct ControlPlaneRequest<S> {
    pub epoch: i64,
    pub changes: Vec<Message<S>>,
    pub all: Vec<S>,
}

impl<S> ControlPlaneRequest<S>
where
    S: Encoder + Decoder + Debug,
{
    pub fn with_changes(epoch: i64, changes: Vec<Message<S>>) -> Self {
        Self {
            epoch,
            changes,
            all: vec![],
        }
    }

    pub fn with_all(epoch: i64, all: Vec<S>) -> Self {
        Self {
            epoch,
            changes: vec![],
            all,
        }
    }
}

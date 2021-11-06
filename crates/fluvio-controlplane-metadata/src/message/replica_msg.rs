#![allow(clippy::assign_op_pattern)]

//!
//! # Replica Messages
//!
//! Replicas are sent from SC to all live SPUs that participate in this replica group.
//! This message is sent for any changes in the live replica group.
//!
//! UPDATE/DEL operation is computed at sender by comparing KV notification with
//! internal metadata cache. Receiver translates UPDATE operations into an ADD/DEL
//! operation the comparing message with internal metadata.
//!

use fluvio_types::SpuId;

use crate::partition::*;

use super::{MsgType, Message, Messages};

pub type ReplicaMsg = Message<Replica>;
pub type ReplicaMsgs = Messages<Replica>;

// -----------------------------------
// ReplicaMsg
// -----------------------------------

impl ReplicaMsg {
    pub fn create_delete_msg(name: ReplicaKey, leader: SpuId) -> Self {
        ReplicaMsg {
            header: MsgType::DELETE,
            content: Replica::new(name, leader, vec![]),
        }
    }
}

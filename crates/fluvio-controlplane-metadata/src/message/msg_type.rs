#![allow(clippy::assign_op_pattern)]

//!
//! # Message Type
//!
//! Message Type is used in Action-Centric messages to label the operation request.
//!
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt;

use fluvio_protocol::{Encoder, Decoder};

use crate::store::actions::*;
use crate::core::*;
use crate::store::*;

#[derive(Decoder, Encoder, Debug, Eq, PartialEq, Clone)]
pub enum MsgType {
    UPDATE,
    DELETE,
}

impl ::std::default::Default for MsgType {
    fn default() -> Self {
        MsgType::UPDATE
    }
}

#[derive(Decoder, Encoder, Debug, Eq, PartialEq, Clone, Default)]
pub struct Message<C> {
    pub header: MsgType,
    pub content: C,
}

impl<C> fmt::Display for Message<C>
where
    C: Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#?} {}", self.header, self.content)
    }
}

impl<C> Message<C> {
    pub fn new(typ: MsgType, content: C) -> Self {
        Message {
            header: typ,
            content,
        }
    }

    pub fn delete(content: C) -> Self {
        Self::new(MsgType::DELETE, content)
    }

    pub fn update(content: C) -> Self {
        Self::new(MsgType::UPDATE, content)
    }
}

/*
impl<C> From<C> for Message<C>
where
    C: Encoder + Decoder + Debug + Default,
{
    fn from(content: C) -> Message<C> {
        Message::update(content)
    }
}
*/

impl<S, C, D> From<LSChange<S, C>> for Message<D>
where
    S: Spec,
    S::Status: PartialEq,
    C: MetadataItem,
    D: From<MetadataStoreObject<S, C>>,
{
    fn from(change: LSChange<S, C>) -> Self {
        match change {
            LSChange::Add(new) => Message::new(MsgType::UPDATE, new.into()),
            LSChange::Mod(new, _old) => Message::new(MsgType::DELETE, new.into()),
            LSChange::Delete(old) => Message::new(MsgType::DELETE, old.into()),
        }
    }
}

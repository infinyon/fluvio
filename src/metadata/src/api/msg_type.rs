//!
//! # Message Type
//!
//! Message Type is used in Action-Centric messages to label the operation request.
//!
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt;

use kf_protocol::derive::{Decode, Encode};
use kf_protocol::{Decoder, Encoder};

#[derive(Decode, Encode, Debug, PartialEq, Clone)]
pub enum MsgType {
    UPDATE,
    DELETE,
}

impl ::std::default::Default for MsgType {
    fn default() -> Self {
        MsgType::UPDATE
    }
}

#[derive(Decode, Encode, Debug, PartialEq, Clone, Default)]
pub struct Message<C>
where
    C: Encoder + Decoder + Debug,
{
    pub header: MsgType,
    pub content: C,
}

impl<C> fmt::Display for Message<C>
where
    C: Encoder + Decoder + Debug + Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#?} {}", self.header, self.content)
    }
}

impl<C> Message<C>
where
    C: Encoder + Decoder + Debug,
{
    pub fn new(typ: MsgType, content: C) -> Self {
        Message {
            header: typ,
            content: content,
        }
    }

    pub fn delete(content: C) -> Self {
        Self::new(MsgType::DELETE, content)
    }

    pub fn update(content: C) -> Self {
        Self::new(MsgType::UPDATE, content)
    }
}

impl<C> From<C> for Message<C>
where
    C: Encoder + Decoder + Debug + Default,
{
    fn from(content: C) -> Message<C> {
        Message::update(content)
    }
}

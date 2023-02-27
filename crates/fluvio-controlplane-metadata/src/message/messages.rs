//!
//! # SmartModule Messages
//!
//! SmartModules are sent from SC to all SPUs.
//!
use std::fmt::{self, Display};
use std::fmt::Debug;

use bytes::Buf;
use fluvio_protocol::{Encoder, Decoder, DecodeFrom, Version};

use super::Message;

#[derive(Encoder, Debug, Eq, PartialEq, Clone, Default)]
pub struct Messages<S> {
    pub messages: Vec<Message<S>>,
}

impl <S> Decoder for Messages<S> 
where Message<S>: DecodeFrom + Decoder
{
    fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), std::io::Error>
    where
        T: Buf {
        let message = Vec::<Message<S>>::decode_from(src, version)?;
        self.messages = message;
        Ok(())
    }
}

impl<S> fmt::Display for Messages<S>
where
    S: Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[")?;
        for sm in &self.messages {
            write!(f, "{sm},")?;
        }
        write!(f, "]")
    }
}

impl<S> Messages<S> {
    pub fn new(messages: Vec<Message<S>>) -> Self {
        Self { messages }
    }

    pub fn push(&mut self, msg: Message<S>) {
        self.messages.push(msg);
    }
}

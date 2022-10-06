//!
//! # SmartModule Messages
//!
//! SmartModules are sent from SC to all SPUs.
//!
use std::fmt::{self, Display};
use std::fmt::Debug;

use fluvio_protocol::{Encoder, Decoder};

use super::Message;

#[derive(Decoder, Encoder, Debug, Eq, PartialEq, Clone, Default)]
pub struct Messages<S>
where
    S: Encoder + Decoder + Debug,
{
    pub messages: Vec<Message<S>>,
}

impl<S> fmt::Display for Messages<S>
where
    S: Encoder + Decoder + Debug + Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[")?;
        for sm in &self.messages {
            write!(f, "{},", sm)?;
        }
        write!(f, "]")
    }
}

impl<S> Messages<S>
where
    S: Encoder + Decoder + Debug,
{
    pub fn new(messages: Vec<Message<S>>) -> Self {
        Self { messages }
    }

    pub fn push(&mut self, msg: Message<S>) {
        self.messages.push(msg);
    }
}

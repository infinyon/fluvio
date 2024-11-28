//! Responsible for communication with SC

mod dispatcher;
mod action;
mod message_sink;

pub use dispatcher::ScDispatcher;

pub use message_sink::*;

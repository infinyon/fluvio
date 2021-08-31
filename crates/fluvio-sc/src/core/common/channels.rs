//!
//! # Channels
//!
//! Helper functions to create a channel
//!
use futures::channel::mpsc::channel;
use futures::channel::mpsc::Receiver;
use futures::channel::mpsc::Sender;

/// Create a channel
pub fn new_channel<T>() -> (Sender<T>, Receiver<T>) {
    channel(100)
}

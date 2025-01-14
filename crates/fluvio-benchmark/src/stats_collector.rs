use std::sync::atomic::{AtomicBool, AtomicU64};

#[derive(Default)]
pub struct ProduceStat {
    pub message_send: AtomicU64,
    pub message_bytes: AtomicU64,
    pub end: AtomicBool,
}

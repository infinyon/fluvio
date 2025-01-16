use std::sync::atomic::AtomicU64;

#[derive(Default, Debug)]
pub struct ProduceStat {
    pub message_send: AtomicU64,
    pub message_bytes: AtomicU64,
}

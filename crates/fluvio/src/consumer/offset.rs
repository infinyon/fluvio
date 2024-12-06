use std::{
    fmt::{Display, Formatter},
    sync::atomic::AtomicI64,
};

use async_channel::{Sender, bounded};
use anyhow::Result;
use fluvio_types::PartitionId;
use serde::Serialize;

use fluvio_protocol::link::ErrorCode;
use fluvio_spu_schema::server::consumer_offset::ConsumerOffset as ConsumerOffsetRequest;

use super::{StreamToServer, StreamToServerCallback};

const DEFAULT_ORDERING: std::sync::atomic::Ordering = std::sync::atomic::Ordering::Relaxed;

pub(crate) struct OffsetLocalStore {
    seen: AtomicI64,
    comitted: AtomicI64,
    flushed: AtomicI64,
    stream_to_server: Sender<StreamToServer>,
}

impl OffsetLocalStore {
    pub(crate) fn new(stream_to_server: Sender<StreamToServer>) -> Self {
        Self {
            seen: AtomicI64::new(-1),
            comitted: AtomicI64::new(-1),
            flushed: AtomicI64::new(-1),
            stream_to_server,
        }
    }

    pub fn update(&self, offset: i64) {
        self.seen.fetch_max(offset, DEFAULT_ORDERING);
    }

    pub fn commit(&self) {
        self.comitted
            .store(self.seen.load(DEFAULT_ORDERING), DEFAULT_ORDERING);
    }

    pub async fn flush(&self) -> Result<(), ErrorCode> {
        if self.flushed() >= self.comitted() {
            return Ok(());
        }
        let (s, r) = bounded(1);
        self.stream_to_server
            .send(StreamToServer::FlushManagedOffset {
                offset: self.comitted(),
                callback: StreamToServerCallback::Channel(s),
            })
            .await
            .map_err(|e| ErrorCode::Other(e.to_string()))?;
        match r
            .recv()
            .await
            .map_err(|e| ErrorCode::Other(e.to_string()))?
        {
            ErrorCode::None => {
                self.set_flushed(self.comitted());
                Ok(())
            }
            other => Err(other),
        }
    }

    pub fn try_flush(&self) -> Result<()> {
        if self.flushed() >= self.comitted() {
            return Ok(());
        }
        self.stream_to_server
            .try_send(StreamToServer::FlushManagedOffset {
                offset: self.comitted(),
                callback: StreamToServerCallback::NoOp,
            })?;
        self.set_flushed(self.comitted());
        Ok(())
    }

    fn flushed(&self) -> i64 {
        self.flushed.load(DEFAULT_ORDERING)
    }

    fn comitted(&self) -> i64 {
        self.comitted.load(DEFAULT_ORDERING)
    }

    fn set_flushed(&self, val: i64) {
        self.flushed.store(val, DEFAULT_ORDERING)
    }
}

impl Display for OffsetLocalStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "OffsetLocalStore {{ seen: {}, comitted: {}, flushed: {} }}",
            self.seen.load(DEFAULT_ORDERING),
            self.comitted.load(DEFAULT_ORDERING),
            self.flushed.load(DEFAULT_ORDERING)
        )
    }
}

#[derive(Debug, Serialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ConsumerOffset {
    pub consumer_id: String,
    pub topic: String,
    pub partition: PartitionId,
    pub offset: i64,
    pub modified_time: u64,
}

impl From<ConsumerOffsetRequest> for ConsumerOffset {
    fn from(value: ConsumerOffsetRequest) -> Self {
        let ConsumerOffsetRequest {
            consumer_id,
            replica_id,
            offset,
            modified_time,
        } = value;

        Self {
            topic: replica_id.topic,
            partition: replica_id.partition,
            consumer_id,
            offset,
            modified_time,
        }
    }
}

#[cfg(test)]
mod tests {
    use async_channel::TryRecvError;

    use super::*;

    #[test]
    fn test_try_flush() {
        //given
        let (sender, recv) = async_channel::bounded(1);
        let store = OffsetLocalStore::new(sender);

        //when
        store.update(1);
        store.commit();
        store.try_flush().expect("flushed");

        //then
        assert!(matches!(
            recv.try_recv(),
            Ok(StreamToServer::FlushManagedOffset { callback: _, offset }) if offset == 1
        ))
    }

    #[test]
    fn test_try_flush_ignores_initial_state() {
        //given
        let (sender, recv) = async_channel::bounded(1);
        let store = OffsetLocalStore::new(sender);

        //when
        store.commit();
        store.try_flush().expect("flushed");

        //then
        assert!(matches!(recv.try_recv(), Err(TryRecvError::Empty)))
    }

    #[test]
    fn test_try_flush_avoid_repeats() {
        //given
        let (sender, recv) = async_channel::bounded(2);
        let store = OffsetLocalStore::new(sender);

        //when
        store.update(1);
        store.commit();
        store.try_flush().expect("flushed");
        store.try_flush().expect("flushed");

        //then
        assert!(matches!(
            recv.try_recv(),
            Ok(StreamToServer::FlushManagedOffset { callback: _, offset }) if offset == 1
        ));
        assert!(matches!(recv.try_recv(), Err(TryRecvError::Empty)))
    }

    #[test]
    fn test_monotonic_update() {
        //given
        let (sender, recv) = async_channel::bounded(1);
        let store = OffsetLocalStore::new(sender);

        //when
        store.update(1);
        store.update(10);
        store.commit();
        store.update(9);
        store.update(8);
        store.commit();
        store.try_flush().expect("flushed");

        //then
        assert!(matches!(
            recv.try_recv(),
            Ok(StreamToServer::FlushManagedOffset { callback: _, offset }) if offset == 10
        ));
        assert!(matches!(recv.try_recv(), Err(TryRecvError::Empty)))
    }

    #[fluvio_future::test]
    async fn test_flush() {
        //given
        let (sender, recv) = async_channel::bounded(2);
        let store = OffsetLocalStore::new(sender);

        fluvio_future::task::spawn(async move {
            //then
            let message1 = recv.recv().await;
            assert!(matches!(
                message1,
                Ok(StreamToServer::FlushManagedOffset { callback: _, offset }) if offset == 1
            ));
            if let Ok(StreamToServer::FlushManagedOffset {
                offset: _,
                callback,
            }) = message1
            {
                callback.send(ErrorCode::None).await;
            }

            let message2 = recv.recv().await;
            assert!(matches!(
                message2,
                Ok(StreamToServer::FlushManagedOffset { callback: _, offset }) if offset == 2
            ));
            if let Ok(StreamToServer::FlushManagedOffset {
                offset: _,
                callback,
            }) = message2
            {
                callback.send(ErrorCode::None).await;
            }
        });

        //when
        store.update(1);
        store.commit();
        store.flush().await.expect("flushed");
        store.update(2);
        store.commit();
        store.flush().await.expect("flushed");
    }

    #[fluvio_future::test]
    async fn test_flush_ignores_initial_state() {
        //given
        let (sender, recv) = async_channel::bounded(1);
        let store = OffsetLocalStore::new(sender);

        //when
        store.commit();
        store.flush().await.expect("flushed");

        //then
        assert!(matches!(recv.try_recv(), Err(TryRecvError::Empty)))
    }

    #[fluvio_future::test]
    async fn test_flush_avoid_repeats() {
        //given
        let (sender, recv) = async_channel::bounded(2);
        let store = OffsetLocalStore::new(sender);

        let recv_clone = recv.clone();
        fluvio_future::task::spawn(async move {
            //then
            let message1 = recv.recv().await;
            assert!(matches!(
                message1,
                Ok(StreamToServer::FlushManagedOffset { callback: _, offset }) if offset == 1
            ));
            if let Ok(StreamToServer::FlushManagedOffset {
                offset: _,
                callback,
            }) = message1
            {
                callback.send(ErrorCode::None).await;
            }
        });

        //when
        store.update(1);
        store.commit();
        store.flush().await.expect("flushed");
        store.flush().await.expect("flushed");

        //then
        assert!(matches!(recv_clone.try_recv(), Err(TryRecvError::Empty)))
    }

    #[fluvio_future::test]
    async fn test_flush_error_propagated() {
        //given
        let (sender, recv) = async_channel::bounded(1);
        let store = OffsetLocalStore::new(sender);

        fluvio_future::task::spawn(async move {
            //then
            let message1 = recv.recv().await;
            assert!(matches!(
                message1,
                Ok(StreamToServer::FlushManagedOffset { callback: _, offset }) if offset == 1
            ));
            if let Ok(StreamToServer::FlushManagedOffset {
                offset: _,
                callback,
            }) = message1
            {
                callback.send(ErrorCode::SpuError).await;
            }
        });

        //when
        store.update(1);
        store.commit();
        let res = store.flush().await;

        //then
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().to_string(), "an error occurred on the SPU");
    }
}

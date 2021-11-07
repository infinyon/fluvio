use std::{collections::HashMap, fmt::Debug, sync::Arc};

use async_lock::Mutex;
use async_trait::async_trait;

use fluvio::spu::{SpuDirectory, SpuSocket};
use fluvio::sockets::VersionedSerialSocket;
use fluvio_types::SpuId;

/// maintain connections to all leaders
#[derive(Debug, Default)]
pub struct LeaderConnections {
    leaders: Arc<Mutex<HashMap<SpuId, SpuSocket>>>,
}

impl LeaderConnections {
    pub fn new() -> Self {
        LeaderConnections {
            ..Default::default()
        }
    }
}

#[async_trait]
impl SpuDirectory for LeaderConnections {
    async fn create_serial_socket(
        &self,
        replica: &dataplane::ReplicaKey,
    ) -> Result<VersionedSerialSocket, fluvio::FluvioError> {
        todo!()
    }

    async fn create_stream_with_version<R: dataplane::api::Request>(
        &self,
        replica: &dataplane::ReplicaKey,
        request: R,
        version: i16,
    ) -> Result<fluvio_socket::AsyncResponse<R>, fluvio::FluvioError>
    where
        R: Sync + Send,
    {
        todo!()
    }
}

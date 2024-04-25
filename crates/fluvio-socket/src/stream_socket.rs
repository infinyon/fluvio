use std::sync::Arc;

use fluvio_protocol::api::{Request, RequestMessage};
use crate::{
    AsyncResponse, ClientConfig, SharedMultiplexerSocket, SocketError, VersionedSerialSocket,
    Versions,
};

const DEFAULT_STREAM_QUEUE_SIZE: usize = 10;

/// Stream Socket
#[derive(Debug)]
pub struct StreamSocket {
    config: Arc<ClientConfig>,
    socket: SharedMultiplexerSocket,
    versions: Versions,
}

impl StreamSocket {
    pub fn new(
        config: Arc<ClientConfig>,
        socket: SharedMultiplexerSocket,
        versions: Versions,
    ) -> Self {
        Self {
            config,
            socket,
            versions,
        }
    }

    pub async fn create_serial_socket(&mut self) -> VersionedSerialSocket {
        VersionedSerialSocket::new(
            self.socket.clone(),
            self.config.clone(),
            self.versions.clone(),
        )
    }

    pub fn is_stale(&self) -> bool {
        self.socket.is_stale()
    }

    pub async fn create_stream_with_version<R: Request>(
        &mut self,
        request: R,
        version: i16,
    ) -> Result<AsyncResponse<R>, SocketError> {
        let mut req_msg = RequestMessage::new_request(request);
        req_msg.header.set_api_version(version);
        req_msg
            .header
            .set_client_id(self.config.client_id().to_owned());
        self.socket
            .create_stream(req_msg, DEFAULT_STREAM_QUEUE_SIZE)
            .await
    }
}

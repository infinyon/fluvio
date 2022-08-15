use std::sync::Arc;

use async_trait::async_trait;
use fluvio_service::ConnectInfo;
use tracing::instrument;
use tracing::{debug, warn};

use fluvio_service::{wait_for_request, FluvioService};
use fluvio_socket::{FluvioSocket, SocketError};

use crate::replication::follower_notifier;
use crate::replication::leader::FollowerHandler;
use super::{SpuPeerRequest, InternalServiceImpl};
use super::SPUPeerApiEnum;
use super::FetchStreamResponse;

#[derive(Debug)]
pub struct InternalService {}

impl InternalService {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl FluvioService for InternalService {
    type Context = InternalServiceImpl;
    type Request = SpuPeerRequest;

    #[instrument(skip(self))]
    async fn respond(
        self: Arc<Self>,
        _ctx: InternalServiceImpl,
        socket: FluvioSocket,
        _connection: ConnectInfo,
    ) -> Result<(), SocketError> {
        let (mut sink, mut stream) = socket.split();
        let mut api_stream = stream.api_stream::<SpuPeerRequest, SPUPeerApiEnum>();

        // register follower
        let (follower_id, spu_update) = wait_for_request!(
            api_stream,

            SpuPeerRequest::FetchStream(req_msg) => {

                let request = &req_msg.request;
                let follower_id = request.spu_id;
                debug!(
                    follower_id,
                    "received fetch stream"
                );
                // check if follower_id is valid
                if let Some(spu_update) = follower_notifier().get(&follower_id).await {
                    let response = FetchStreamResponse::new(follower_id);
                    let res_msg = req_msg.new_response(response);
                    sink
                        .send_response(&res_msg, req_msg.header.api_version())
                        .await?;
                    (follower_id,spu_update)
                } else {
                    warn!(follower_id, "unknown spu, dropping connection");
                    return Ok(())
                }

            }
        );

        drop(api_stream);

        FollowerHandler::start(follower_id, spu_update, sink, stream).await;

        debug!("finishing SPU peer loop");
        Ok(())
    }
}

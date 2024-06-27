use std::sync::Arc;

use async_trait::async_trait;
use tracing::trace;
use tracing::{debug, warn, instrument};
use anyhow::Result;

use fluvio_service::{wait_for_request, FluvioService, ConnectInfo};
use fluvio_socket::FluvioSocket;

use crate::core::DefaultSharedGlobalContext;
use crate::replication::leader::FollowerHandler;
use crate::services::internal::fetch_consumer_offset_handler::handle_fetch_consumer_offset_request;
use crate::services::internal::update_consumer_offset_handler::handle_update_consumer_offset_request;
use super::SpuPeerRequest;
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
    type Context = DefaultSharedGlobalContext;
    type Request = SpuPeerRequest;

    #[instrument(skip(self, ctx))]
    async fn respond(
        self: Arc<Self>,
        ctx: DefaultSharedGlobalContext,
        socket: FluvioSocket,
        _connection: ConnectInfo,
    ) -> Result<()> {
        let (mut sink, mut stream) = socket.split();
        let mut api_stream = stream.api_stream::<SpuPeerRequest, SPUPeerApiEnum>();

        let spu_id = ctx.local_spu_id();

        // register follower
        wait_for_request!(
            api_stream,

            SpuPeerRequest::FetchStream(req_msg) => {

                let request = &req_msg.request;
                let follower_id = request.spu_id;
                let leader_spu_id = request.leader_spu_id;
                debug!(
                    follower_id,
                    "received fetch stream"
                );

                if leader_spu_id != spu_id || follower_id == spu_id {
                    warn!(follower_id, spu_id, "spu id does not match, dropping connection");
                    let response = FetchStreamResponse::new(None);
                    let res_msg = req_msg.new_response(response);
                    sink
                        .send_response(&res_msg, req_msg.header.api_version())
                        .await?;
                }


                // check if follower_id is valid
                if let Some(spu_update) = ctx.follower_notifier().get(&follower_id).await {
                    let response = FetchStreamResponse::new(Some(follower_id));
                    let res_msg = req_msg.new_response(response);
                    sink
                        .send_response(&res_msg, req_msg.header.api_version())
                        .await?;
                    drop(api_stream);
                    FollowerHandler::start(ctx, follower_id, spu_update, sink, stream).await;
                } else {
                    warn!(follower_id, "unknown spu, dropping connection");
                    let response = FetchStreamResponse::new(None);
                    let res_msg = req_msg.new_response(response);
                    sink
                        .send_response(&res_msg, req_msg.header.api_version())
                        .await?;
                }

            },
            SpuPeerRequest::FetchConsumerOffset(req_msg) => {
                debug!(consumer_id = req_msg.request.consumer_id, replica = %req_msg.request.replica_id, "fetch consumer offset request");
                let api_version = req_msg.header.api_version();
                let response = handle_fetch_consumer_offset_request(req_msg, ctx).await?;
                sink.send_response(&response, api_version).await?;
            },
            SpuPeerRequest::UpdateConsumerOffset(req_msg) => {
                trace!(consumer_id = req_msg.request.consumer_id, replica = %req_msg.request.replica_id, "update consumer offset request");
                let api_version = req_msg.header.api_version();
                let response = handle_update_consumer_offset_request(req_msg, ctx).await?;
                sink.send_response(&response, api_version).await?;
            }

        );

        debug!("finishing SPU peer loop");
        Ok(())
    }
}

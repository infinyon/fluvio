use std::sync::Arc;

use async_trait::async_trait;
use tracing::trace;
use tracing::{debug, warn, instrument};
use anyhow::Result;

use fluvio_service::{wait_for_request, FluvioService, ConnectInfo};
use fluvio_socket::FluvioSocket;

use crate::core::DefaultSharedGlobalContext;
use crate::replication::leader::FollowerHandler;
use crate::services::internal::fetch_consumer_handler::handle_fetch_consumer_request;
use crate::services::internal::update_consumer_handler::handle_update_consumer_request;
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

        // register follower
        wait_for_request!(
            api_stream,

            SpuPeerRequest::FetchStream(req_msg) => {

                let request = &req_msg.request;
                let follower_id = request.spu_id;
                debug!(
                    follower_id,
                    "received fetch stream"
                );
                // check if follower_id is valid
                if let Some(spu_update) = ctx.follower_notifier().get(&follower_id).await {
                    let response = FetchStreamResponse::new(follower_id);
                    let res_msg = req_msg.new_response(response);
                    sink
                        .send_response(&res_msg, req_msg.header.api_version())
                        .await?;
                    drop(api_stream);
                    FollowerHandler::start(ctx, follower_id, spu_update, sink, stream).await;
                } else {
                    warn!(follower_id, "unknown spu, dropping connection");
                    return Ok(())
                }

            },
            SpuPeerRequest::FetchConsumer(req_msg) => {
                debug!(consumer_id = req_msg.request.consumer_id, topic=req_msg.request.topic, partition = req_msg.request.partition,
                       "fetch consumer request");
                let api_version = req_msg.header.api_version();
                let response = handle_fetch_consumer_request(req_msg, ctx).await?;
                sink.send_response(&response, api_version).await?;
            },
            SpuPeerRequest::UpdateConsumer(req_msg) => {
                trace!(consumer_id = req_msg.request.consumer_id, topic=req_msg.request.topic, partition = req_msg.request.partition,
                       "update consumer request");
                let api_version = req_msg.header.api_version();
                let response = handle_update_consumer_request(req_msg, ctx).await?;
                sink.send_response(&response, api_version).await?;
            }

        );

        debug!("finishing SPU peer loop");
        Ok(())
    }
}

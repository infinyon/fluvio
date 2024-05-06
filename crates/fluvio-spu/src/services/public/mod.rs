mod api_versions;
mod produce_handler;
mod fetch_handler;
mod offset_request;
mod offset_update;
mod stream_fetch;
mod consumer_handler;

#[cfg(test)]
mod tests;
mod conn_context;

use std::sync::Arc;
use async_trait::async_trait;
use fluvio_protocol::api::Request;
use fluvio_protocol::api::RequestMessage;
use fluvio_protocol::link::ErrorCode;
use fluvio_protocol::record::ReplicaKey;
use fluvio_spu_schema::server::mirror::StartMirrorRequest;
use tracing::{info, debug, trace, instrument};
use futures_util::StreamExt;
use anyhow::Result;

use fluvio_socket::FluvioSocket;
use fluvio_service::{FluvioApiServer, FluvioService, ConnectInfo, call_service};
use fluvio_spu_schema::server::SpuServerRequest;
use fluvio_spu_schema::server::SpuServerApiKey;
use fluvio_types::event::StickyEvent;

use crate::core::DefaultSharedGlobalContext;
use crate::mirroring::home::connection::MirrorHomeHandler;
use crate::services::public::consumer_handler::handle_delete_consumer_offset_request;
use crate::services::public::consumer_handler::handle_fetch_consumer_offsets_request;
use crate::services::public::consumer_handler::handle_update_consumer_offset_request;
use self::api_versions::handle_api_version_request;
use self::produce_handler::handle_produce_request;
use self::fetch_handler::handle_fetch_request;
use self::offset_request::handle_offset_request;
use self::offset_update::handle_offset_update;
use self::stream_fetch::{StreamFetchHandler, publishers::StreamPublishers};
use self::conn_context::ConnectionContext;

pub(crate) type SpuPublicServer =
    FluvioApiServer<SpuServerRequest, SpuServerApiKey, DefaultSharedGlobalContext, PublicService>;

pub fn create_public_server(addr: String, ctx: DefaultSharedGlobalContext) -> SpuPublicServer {
    info!(
        spu_id = ctx.local_spu_id(),
        %addr,
        "Starting SPU public service:",
    );

    FluvioApiServer::new(addr, ctx, PublicService::new())
}

#[derive(Debug)]
pub struct PublicService {
    _0: (), // Prevent construction
}

impl PublicService {
    pub fn new() -> Self {
        PublicService { _0: () }
    }
}

#[async_trait]
impl FluvioService for PublicService {
    type Request = SpuServerRequest;
    type Context = DefaultSharedGlobalContext;

    #[instrument(skip(self, context))]
    async fn respond(
        self: Arc<Self>,
        context: DefaultSharedGlobalContext,
        socket: FluvioSocket,
        _connection: ConnectInfo,
    ) -> Result<()> {
        let (sink, mut stream) = socket.split();

        let mut mirror_request: Option<RequestMessage<StartMirrorRequest>> = None;
        let shutdown = StickyEvent::shared();

        let mut shared_sink = sink.as_shared();
        {
            let api_stream = stream.api_stream::<SpuServerRequest, SpuServerApiKey>();
            let mut event_stream = api_stream.take_until(shutdown.listen_pinned());
            let mut conn_ctx = ConnectionContext::new();

            loop {
                let event = event_stream.next().await;
                match event {
                    Some(Ok(req_message)) => {
                        debug!(%req_message,"received");
                        //  println!("req: {:#?}", req_message);
                        trace!(
                            "conn: {}, received request: {:#?}",
                            shared_sink.id(),
                            req_message
                        );
                        match req_message {
                            SpuServerRequest::ApiVersionsRequest(request) => call_service!(
                                request,
                                handle_api_version_request(request),
                                shared_sink,
                                "ApiVersionsRequest"
                            ),
                            SpuServerRequest::ProduceRequest(request) => call_service!(
                                request,
                                handle_produce_request(request, context.clone()),
                                shared_sink,
                                "ProduceRequest"
                            ),
                            SpuServerRequest::FileFetchRequest(request) => {
                                handle_fetch_request(request, context.clone(), shared_sink.clone())
                                    .await?
                            }
                            SpuServerRequest::FetchOffsetsRequest(request) => call_service!(
                                request,
                                handle_offset_request(request, context.clone()),
                                shared_sink,
                                "FetchOffsetsRequest"
                            ),
                            SpuServerRequest::FileStreamFetchRequest(request) => {
                                StreamFetchHandler::start(
                                    request,
                                    context.clone(),
                                    &mut conn_ctx,
                                    shared_sink.clone(),
                                    shutdown.clone(),
                                )
                                .await?;
                            }
                            SpuServerRequest::UpdateOffsetsRequest(request) => call_service!(
                                request,
                                handle_offset_update(request, &mut conn_ctx),
                                shared_sink,
                                "UpdateOffsetsRequest"
                            ),
                            SpuServerRequest::UpdateConsumerOffsetRequest(request) => {
                                call_service!(
                                    request,
                                    handle_update_consumer_offset_request(
                                        request,
                                        context.clone(),
                                        &mut conn_ctx
                                    ),
                                    shared_sink,
                                    "UpdateConsumerRequest"
                                )
                            }
                            SpuServerRequest::DeleteConsumerOffsetRequest(request) => {
                                call_service!(
                                    request,
                                    handle_delete_consumer_offset_request(request, context.clone()),
                                    shared_sink,
                                    "DeleteConsumerRequest"
                                )
                            }
                            SpuServerRequest::FetchConsumerOffsetsRequest(request) => {
                                call_service!(
                                    request,
                                    handle_fetch_consumer_offsets_request(request, context.clone()),
                                    shared_sink,
                                    "FetchConsumersRequest"
                                )
                            }
                            SpuServerRequest::StartMirrorRequest(request) => {
                                // send mirror mode, afer that mirror cycle will be started
                                mirror_request = Some(request);
                                break;
                            }
                        }
                    }
                    Some(Err(e)) => {
                        debug!(
                            sink_id = shared_sink.id(),
                            "Error decoding message, ending connection: {}", e
                        );
                        break;
                    }
                    None => {
                        debug!(sink_id = shared_sink.id(), "No content, end of connection",);
                        break;
                    }
                }
            }
        }

        if let Some(request) = mirror_request {
            MirrorHomeHandler::respond(context, request, shared_sink, &mut stream).await;
        }

        shutdown.notify();
        debug!("service terminated");
        Ok(())
    }
}

async fn send_private_request_to_leader<R: Request>(
    ctx: &DefaultSharedGlobalContext,
    replica_id: &ReplicaKey,
    req: R,
) -> Result<R::Response, ErrorCode> {
    let spu = match ctx.replica_localstore().spec(replica_id) {
        Some(replica) => replica.leader,
        None => return Err(ErrorCode::TopicNotFound),
    };
    let Some(spu_spec) = ctx.spu_localstore().spec(&spu) else {
        return Err(ErrorCode::SpuNotFound);
    };
    let leader_endpoint = spu_spec.private_endpoint.to_string();
    debug!(
        spu,
        leader_endpoint, "send private request to replica leader"
    );
    let mut socket = FluvioSocket::connect(&leader_endpoint)
        .await
        .map_err(|e| ErrorCode::Other(e.to_string()))?;

    let req_msg = RequestMessage::new_request(req);
    let response = socket
        .send(&req_msg)
        .await
        .map_err(|e| ErrorCode::Other(e.to_string()))?;

    Ok(response.response)
}

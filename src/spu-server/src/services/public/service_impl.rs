use std::sync::Arc;
use std::collections::HashSet;

use tracing::debug;
use tracing::trace;
use tracing::warn;
use async_trait::async_trait;
use futures::io::AsyncRead;
use futures::io::AsyncWrite;
use futures::stream::StreamExt;
use tokio::select;

use kf_protocol::api::RequestMessage;
use kf_socket::InnerKfSocket;
use kf_socket::InnerKfSink;
use kf_socket::KfSocketError;
use kf_service::call_service;
use kf_service::KfService;
use spu_api::server::SpuServerApiKey;
use spu_api::server::SpuServerRequest;
use flv_future_aio::zero_copy::ZeroCopyWrite;

use crate::core::DefaultSharedGlobalContext;
use super::api_versions::handle_kf_lookup_version_request;
use super::produce_handler::handle_produce_request;
use super::fetch_handler::handle_fetch_request;
use super::offset_request::handle_offset_request;
use super::stream_fetch::StreamFetchHandler;
use super::OffsetReplicaList;


#[derive(Debug)]
pub struct PublicService {}

impl PublicService {
    pub fn new() -> Self {
        PublicService {}
    }
}

#[async_trait]
impl<S> KfService<S> for PublicService
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Context = DefaultSharedGlobalContext;
    type Request = SpuServerRequest;

    async fn respond(
        self: Arc<Self>,
        context: DefaultSharedGlobalContext,
        socket: InnerKfSocket<S>,
    ) -> Result<(), KfSocketError>
    where
        InnerKfSink<S>: ZeroCopyWrite,
    {
        let (sink, mut stream) = socket.split();

        let mut s_sink = sink.as_shared();
        let mut api_stream = stream.api_stream::<SpuServerRequest, SpuServerApiKey>();

        let mut offset_replica_list: OffsetReplicaList = HashSet::new();

        let mut receiver = context.offset_channel().receiver();

        loop {
            select! {
                offset_event_res = receiver.recv() => {

                    match offset_event_res {

                        Ok(offset_event) => {
                            trace!("conn: {}, offset event from leader {:#?}", s_sink.id(),offset_event);
                            if offset_replica_list.contains(&offset_event.replica_id) {

                                use spu_api::client::offset::ReplicaOffsetUpdateRequest;
                                use spu_api::client::offset::ReplicaOffsetUpdate;
                                use kf_protocol::api::FlvErrorCode;

                                debug!("conn: {}, sending replica: {} hw: {}, leo: {}",s_sink.id(),
                                    offset_event.replica_id,
                                    offset_event.hw,
                                    offset_event.leo);

                                let req = ReplicaOffsetUpdateRequest {
                                    offsets: vec![ReplicaOffsetUpdate {
                                        replica: offset_event.replica_id,
                                        error_code: FlvErrorCode::None,
                                        start_offset: 0,
                                        leo: offset_event.leo,
                                        hw: offset_event.hw
                                    }]
                                };
                                s_sink.send_request(&RequestMessage::new_request(req)).await?;

                            }
                        },

                        Err(err) => {

                            use flv_future_aio::sync::broadcast::RecvError;

                            match err {
                                RecvError::Closed => {
                                    warn!("conn: {}, lost connection to event channel, closing conn",s_sink.id());
                                    break;
                                },
                                RecvError::Lagged(lag) => {
                                    warn!("conn: {}, lagging: {}",s_sink.id(),lag);
                                }
                            }

                        }

                    }
                },


                api_msg = api_stream.next() => {

                    if let Some(msg) = api_msg {

                        if let Ok(req_message) = msg {
                            trace!("conn: {}, received request: {:#?}",s_sink.id(),req_message);
                            match req_message {
                                SpuServerRequest::ApiVersionsRequest(request) => call_service!(
                                    request,
                                    handle_kf_lookup_version_request(request),
                                    s_sink,
                                    "kf api version handler"
                                ),

                                // Kafka
                                SpuServerRequest::KfProduceRequest(request) => call_service!(
                                    request,
                                    handle_produce_request(request,context.clone()),
                                    s_sink,
                                    "ks produce request handler"
                                ),
                                SpuServerRequest::KfFileFetchRequest(request) => handle_fetch_request(request,context.clone(),s_sink.clone()).await?,

                                SpuServerRequest::FlvFetchOffsetsRequest(request) => call_service!(
                                    request,
                                    handle_offset_request(request,context.clone()),
                                    s_sink,
                                    "handling offset fetch request"
                                ),

                                SpuServerRequest::RegisterSyncReplicaRequest(request) => {
                                    use std::iter::FromIterator;

                                    let (_, sync_request) = request.get_header_request();
                                    debug!("registered offset sync request: {:#?}",sync_request);
                                    offset_replica_list = HashSet::from_iter(sync_request.leader_replicas);
                                },
                                SpuServerRequest::FileStreamFetchRequest(request) =>  StreamFetchHandler::handle_stream_fetch(request,context.clone(),s_sink.clone()).await?

                            }
                        } else {
                            tracing::debug!("conn: {} msg can't be decoded, ending connection",s_sink.id());
                            break;
                        }
                    } else {
                        tracing::debug!("conn: {}, no content, end of connection", s_sink.id());
                        break;
                    }

                }

            }
        }

        debug!("conn: {}, loop terminated ", s_sink.id());
        Ok(())
    }
}

use std::sync::Arc;

use tracing::{debug, trace, instrument};
use async_trait::async_trait;
use futures_util::stream::StreamExt;
use tokio::select;

use fluvio_types::event::SimpleEvent;
use fluvio_socket::FluvioSocket;
use fluvio_socket::SocketError;
use fluvio_service::{call_service, FlvService};
use fluvio_spu_schema::server::{SpuServerApiKey, SpuServerRequest};
use dataplane::{ErrorCode, api::RequestMessage};

use crate::core::DefaultSharedGlobalContext;
use super::api_versions::handle_kf_lookup_version_request;
use super::produce_handler::handle_produce_request;
use super::fetch_handler::handle_fetch_request;
use super::offset_request::handle_offset_request;
use super::stream_fetch::StreamFetchHandler;

#[derive(Debug)]
pub struct PublicService {}

impl PublicService {
    pub fn new() -> Self {
        PublicService {}
    }
}

#[async_trait]
impl FlvService for PublicService {
    type Context = DefaultSharedGlobalContext;
    type Request = SpuServerRequest;

    #[instrument(skip(self, context, socket))]
    async fn respond(
        self: Arc<Self>,
        context: DefaultSharedGlobalContext,
        socket: FluvioSocket,
    ) -> Result<(), SocketError> {
        let (sink, mut stream) = socket.split();

        let mut s_sink = sink.as_shared();
        let mut api_stream = stream.api_stream::<SpuServerRequest, SpuServerApiKey>();

        let end_event = SimpleEvent::shared();

        loop {
            select! {

                _ = end_event.listen() => {
                    debug!("end event has been received from stream fetch, terminating");
                    break;
                },

                api_msg = api_stream.next() => {

                    if let Some(msg) = api_msg {

                        if let Ok(req_message) = msg {
                            debug!(sink = s_sink.id(),"received request");
                            trace!("conn: {}, received request: {:#?}",s_sink.id(),req_message);
                            match req_message {
                                SpuServerRequest::ApiVersionsRequest(request) => call_service!(
                                    request,
                                    handle_kf_lookup_version_request(request),
                                    s_sink,
                                    "api version handler"
                                ),


                                SpuServerRequest::ProduceRequest(request) => call_service!(
                                    request,
                                    handle_produce_request(request,context.clone()),
                                    s_sink,
                                    "roduce request handler"
                                ),
                                SpuServerRequest::FileFetchRequest(request) => handle_fetch_request(request,context.clone(),s_sink.clone()).await?,

                                SpuServerRequest::FetchOffsetsRequest(request) => call_service!(
                                    request,
                                    handle_offset_request(request,context.clone()),
                                    s_sink,
                                    "handling offset fetch request"
                                ),

                                SpuServerRequest::FileStreamFetchRequest(request) =>  {

                                        StreamFetchHandler::start(
                                            request,
                                            context.clone(),
                                            s_sink.clone(),
                                            end_event.clone(),
                                        ).await?;


                                },
                                SpuServerRequest::UpdateOffsetsRequest(request) =>
                                    call_service!(
                                        request,
                                        offset::handle_offset_update(&context,request),
                                        s_sink,
                                        "roduce request handler"
                                    ),

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

        end_event.notify();

        debug!("conn: {}, loop terminated ", s_sink.id());
        Ok(())
    }
}

mod offset {

    use std::{io::Error as IoError};

    use tracing::{debug, error, instrument};
    use fluvio_spu_schema::server::update_offset::{
        OffsetUpdateStatus, UpdateOffsetsRequest, UpdateOffsetsResponse,
    };
    use dataplane::api::ResponseMessage;

    use super::{DefaultSharedGlobalContext, RequestMessage, ErrorCode};

    #[instrument(skip(ctx, request))]
    pub async fn handle_offset_update(
        ctx: &DefaultSharedGlobalContext,
        request: RequestMessage<UpdateOffsetsRequest>,
    ) -> Result<ResponseMessage<UpdateOffsetsResponse>, IoError> {
        debug!("received stream updates");
        let (header, updates) = request.get_header_request();
        let publishers = ctx.stream_publishers();
        let mut status_list = vec![];

        for update in updates.offsets {
            let status = if let Some(publisher) = publishers.get_publisher(update.session_id).await
            {
                debug!(
                    offset_update = update.offset,
                    session_id = update.session_id,
                    "published offsets"
                );
                publisher.update(update.offset);
                OffsetUpdateStatus {
                    session_id: update.session_id,
                    error: ErrorCode::None,
                }
            } else {
                error!(
                    "invalid offset {}, session_id:{}",
                    update.offset, update.session_id
                );
                OffsetUpdateStatus {
                    session_id: update.session_id,
                    error: ErrorCode::FetchSessionNotFoud,
                }
            };
            status_list.push(status);
        }
        let response = UpdateOffsetsResponse {
            status: status_list,
        };
        Ok(RequestMessage::<UpdateOffsetsRequest>::response_with_header(&header, response))
    }
}

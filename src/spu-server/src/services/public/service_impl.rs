use std::sync::Arc;

use async_trait::async_trait;
use futures::io::AsyncRead;
use futures::io::AsyncWrite;

use kf_socket::InnerKfSocket;
use kf_socket::InnerKfSink;
use kf_socket::KfSocketError;
use kf_service::call_service;
use kf_service::KfService;
use kf_service::api_loop;
use spu_api::SpuApiKey;
use spu_api::PublicRequest;
use flv_future_aio::zero_copy::ZeroCopyWrite;

use crate::core::DefaultSharedGlobalContext;
use super::api_versions::handle_kf_lookup_version_request;
use super::produce_handler::handle_produce_request;
use super::fetch_handler::handle_fetch_request;
use super::cf_handler::CfHandler;
use super::local_spu_request::handle_spu_request;
use super::offset_request::handle_offset_request;

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
    type Request = PublicRequest;

    async fn respond(
        self: Arc<Self>,
        context: DefaultSharedGlobalContext,
        socket: InnerKfSocket<S>,
    ) -> Result<(), KfSocketError>
    where
        InnerKfSink<S>: ZeroCopyWrite,
    {
        let (mut sink, mut stream) = socket.split();
        let mut api_stream = stream.api_stream::<PublicRequest, SpuApiKey>();

        api_loop!(
            api_stream,

            // Mixed
            PublicRequest::ApiVersionsRequest(request) => call_service!(
                request,
                handle_kf_lookup_version_request(request),
                sink,
                "kf api version handler"
            ),

            // Kafka
            PublicRequest::KfProduceRequest(request) => call_service!(
                request,
                handle_produce_request(request,context.clone()),
                sink,
                "ks produce request handler"
            ),
            PublicRequest::KfFileFetchRequest(request) => handle_fetch_request(request,context.clone(),&mut sink).await?,

            // Fluvio
            PublicRequest::FlvFetchLocalSpuRequest(request) => call_service!(
                request,
                handle_spu_request(request,context.clone()),
                sink,
                "handling local spu request"
            ),
            PublicRequest::FlvFetchOffsetsRequest(request) => call_service!(
                request,
                handle_offset_request(request,context.clone()),
                sink,
                "handling offset fetch request"
            ),
            PublicRequest::FileFlvContinuousFetchRequest(request) => {

                drop(api_stream);
                CfHandler::handle_continuous_fetch_request(request,context.clone(),sink,stream).await?;
                break;

            }

        );

        Ok(())
    }
}

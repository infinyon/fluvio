
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::future::FutureExt;

use kf_socket::KfSocket;
use kf_socket::KfSocketError;
use kf_service::call_service;
use kf_service::KfService;
use kf_service::api_loop;
use spu_api::SpuApiKey;
use spu_api::PublicRequest;

use crate::core::DefaultSharedGlobalContext;
use super::api_versions::handle_kf_lookup_version_request;
use super::produce_handler::handle_produce_request;
use super::fetch_handler::handle_fetch_request;
use super::local_spu_request::handle_spu_request;
use super::offset_request::handle_offset_request;

pub struct PublicService {
}

impl PublicService {

    pub fn new() -> Self {
        PublicService{}
    }

    async fn handle(self: Arc<Self>, context: DefaultSharedGlobalContext, socket: KfSocket) -> Result<(),KfSocketError> {

        let (mut sink,mut stream) = socket.split();
        let mut api_stream = stream.api_stream::<PublicRequest,SpuApiKey>();

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
            )
        );

        Ok(())


    }
}


impl KfService for PublicService {
    type Context = DefaultSharedGlobalContext;
    type Request = PublicRequest;
    type ResponseFuture = BoxFuture<'static,Result<(),KfSocketError>>;
  
    fn respond(
        self: Arc<Self>,
        context: DefaultSharedGlobalContext,
        socket: KfSocket
    ) -> Self::ResponseFuture {
       
        self.handle(context,socket).boxed()
    }

}

use std::sync::Arc;

use async_trait::async_trait;

use kf_service::api_loop;
use kf_service::KfService;
use kf_socket::KfSocket;
use kf_socket::KfSocketError;

use super::SpuPeerRequest;
use super::KfSPUPeerApiEnum;

use super::fetch_stream::handle_fetch_stream_request;
use crate::core::DefaultSharedGlobalContext;

pub struct SpunternalService {}

impl SpunternalService {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl KfService for SpunternalService {

    type Context = DefaultSharedGlobalContext;
    type Request = SpuPeerRequest;
   
    async fn respond(
        self: Arc<Self>,
        context: DefaultSharedGlobalContext,
        socket: KfSocket,
    ) -> Result<(), KfSocketError> {

        let (sink, mut stream) = socket.split();
        let mut api_stream = stream.api_stream::<SpuPeerRequest, KfSPUPeerApiEnum>();

        api_loop!(
            api_stream,

            SpuPeerRequest::FetchStream(request) => {

                drop(api_stream);
                let orig_socket: KfSocket  = (sink,stream).into();
                handle_fetch_stream_request(request, context, orig_socket).await?;
                return Ok(());

            }
        );

        Ok(())
    }
}

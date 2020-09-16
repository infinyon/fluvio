use std::sync::Arc;

use async_trait::async_trait;

use kf_service::api_loop;
use kf_service::KfService;
use kf_socket::KfSocket;
use kf_socket::KfSocketError;
use flv_future_aio::net::TcpStream;

use super::SpuPeerRequest;
use super::SPUPeerApiEnum;

use super::fetch_stream::handle_fetch_stream_request;
use crate::core::DefaultSharedGlobalContext;

#[derive(Debug)]
pub struct InternalService {}

impl InternalService {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl KfService<TcpStream> for InternalService {
    type Context = DefaultSharedGlobalContext;
    type Request = SpuPeerRequest;

    async fn respond(
        self: Arc<Self>,
        context: DefaultSharedGlobalContext,
        socket: KfSocket,
    ) -> Result<(), KfSocketError> {
        let (sink, mut stream) = socket.split();
        let mut api_stream = stream.api_stream::<SpuPeerRequest, SPUPeerApiEnum>();

        api_loop!(
            api_stream,

            SpuPeerRequest::FetchStream(request) => {

                drop(api_stream);
                let orig_socket: KfSocket  = (sink,stream).into();
                handle_fetch_stream_request(request, context, orig_socket).await?;
                break;

            }
        );

        Ok(())
    }
}

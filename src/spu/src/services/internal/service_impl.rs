use std::sync::Arc;

use async_trait::async_trait;

use fluvio_service::api_loop;
use fluvio_service::FlvService;
use fluvio_socket::FlvSocket;
use fluvio_socket::FlvSocketError;
use fluvio_future::net::TcpStream;

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
impl FlvService<TcpStream> for InternalService {
    type Context = DefaultSharedGlobalContext;
    type Request = SpuPeerRequest;

    async fn respond(
        self: Arc<Self>,
        context: DefaultSharedGlobalContext,
        socket: FlvSocket,
    ) -> Result<(), FlvSocketError> {
        let (sink, mut stream) = socket.split();
        let mut api_stream = stream.api_stream::<SpuPeerRequest, SPUPeerApiEnum>();

        api_loop!(
            api_stream,

            SpuPeerRequest::FetchStream(request) => {

                drop(api_stream);
                let orig_socket: FlvSocket  = (sink,stream).into();
                handle_fetch_stream_request(request, context, orig_socket).await?;
                break;

            }
        );

        Ok(())
    }
}



use std::net::SocketAddr;
use std::sync::Arc;


use tracing::info;
use tracing::debug;
use futures::future::BoxFuture;
use futures::future::FutureExt;


use fluvio_controlplane::InternalScKey;
use fluvio_controlplane::InternalScRequest;
use fluvio_controlplane::RegisterSpuResponse;
use fluvio_socket::FlvSocket;
use fluvio_socket::FlvSocketError;
use fluvio_service::FlvApiServer;
use fluvio_service::FlvService;
use fluvio_service::wait_for_request;


use super::SpuTest;
use super::SpuTestRunner;


pub type SharedScContext = Arc<ScGlobalContext>;

pub(crate) type MockScServer<T> = FlvApiServer<
        InternalScRequest,
        InternalScKey,
        SharedScContext,
        MockInternalService<T>>;


#[derive(Debug)]
pub struct ScGlobalContext {
    
}

impl ScGlobalContext {

    pub fn new_shared_context() ->  SharedScContext {
        Arc::new(ScGlobalContext{})
    }
    
    pub fn create_server<T>(self: Arc<Self>,addr: SocketAddr,test_runner: Arc<SpuTestRunner<T>>) -> MockScServer<T> 
        where T: SpuTest + Sync + Send + 'static {
        
        info!("starting mock SC service at: {:#?}", addr);

        FlvApiServer::new(addr, self.clone(), MockInternalService::new(test_runner))
    }
}

unsafe impl <T>Sync for MockInternalService<T>{}

pub struct MockInternalService<T>(Arc<SpuTestRunner<T>>);



impl <T>MockInternalService<T> where T: SpuTest + Sync + Send+ 'static {
    pub fn new(test_runner: Arc<SpuTestRunner<T>>) -> Self {
        Self(test_runner)
    }

    async fn handle(
        self: Arc<Self>,
        _context: SharedScContext,
        socket: FlvSocket,
    ) -> Result<(), FlvSocketError> {

        let (mut sink, mut stream) = socket.split();
        let mut api_stream = stream.api_stream::<InternalScRequest, InternalScKey>();

        // wait for spu registeration
        let spu_id = wait_for_request!(api_stream,
            InternalScRequest::RegisterSpuRequest(req_msg) => {
                
                let spu_id = req_msg.request.spu();
                debug!("registration req from spu: {}",spu_id);
                let response = req_msg.new_response(RegisterSpuResponse{});
                sink.send_response(&response,req_msg.header.api_version()).await?;
                spu_id
                
            }
        );

        self.0.send_metadata_to_spu(&mut sink,spu_id).await.expect("send metadata should work");


        Ok(())

    }
}


impl <T>FlvService for MockInternalService<T> where T: SpuTest + Sync + Send + 'static {
    type Context = SharedScContext;
    type Request = InternalScRequest;
    type ResponseFuture = BoxFuture<'static,Result<(),FlvSocketError>>;

    fn respond(
        self: Arc<Self>,
        context: SharedScContext,
        socket: FlvSocket,
    ) -> Self::ResponseFuture {
        self.handle(context, socket).boxed()
    }
}



use std::net::SocketAddr;
use std::sync::Arc;


use tracing::info;
use tracing::debug;
use futures::future::BoxFuture;
use futures::future::FutureExt;


use internal_api::InternalScKey;
use internal_api::InternalScRequest;
use internal_api::RegisterSpuResponse;
use kf_socket::KfSocket;
use kf_socket::KfSocketError;
use kf_service::KfApiServer;
use kf_service::KfService;
use kf_service::wait_for_request;


use super::SpuTest;
use super::SpuTestRunner;


pub type SharedScContext = Arc<ScGlobalContext>;

pub(crate) type MockScServer<T> = KfApiServer<
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

        KfApiServer::new(addr, self.clone(), MockInternalService::new(test_runner))
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
        socket: KfSocket,
    ) -> Result<(), KfSocketError> {

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


impl <T>KfService for MockInternalService<T> where T: SpuTest + Sync + Send + 'static {
    type Context = SharedScContext;
    type Request = InternalScRequest;
    type ResponseFuture = BoxFuture<'static,Result<(),KfSocketError>>;

    fn respond(
        self: Arc<Self>,
        context: SharedScContext,
        socket: KfSocket,
    ) -> Self::ResponseFuture {
        self.handle(context, socket).boxed()
    }
}

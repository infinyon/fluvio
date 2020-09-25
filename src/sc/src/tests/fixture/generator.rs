use std::sync::Arc;

use tracing::debug;
use futures::channel::mpsc::Receiver;
use futures::channel::mpsc::channel;
use futures::channel::mpsc::Sender;
use futures::SinkExt;

use error::ServerError;
use fluvio_types::socket_helpers::EndPoint;
use fluvio_socket::FlvSocket;
use fluvio_socket::FlvSocketError;
use kf_protocol::api::Request;
use kf_protocol::api::RequestMessage;
use kf_protocol::api::ResponseMessage;
use utils::actions::Actions;

use crate::cli::ScConfig;
use crate::core::SharedScMetadata;
use crate::create_core_services;
use crate::services::InternalApiServer;
use crate::core::ScRequest;
use crate::core::ScRequestSender;
use crate::core::HcActionSender;
use crate::hc_manager::HcAction;

use super::mock_spu::SpuGlobalContext;
use super::mock_spu::SharedSpuContext;
use super::ScTestRunner;
use super::ScTest;
use super::SpuSpec;
use super::SharedKVStore;

#[derive(Default)]
pub struct TestGenerator {

    base_id: i32,
    base_port: u16,
    total_spu: usize,           // total number of spu
    init_spu: usize             // initial spu to start
}


impl TestGenerator  {

    pub fn set_base_id(mut self,id: i32) -> Self {
        self.base_id = id;
        self
    }

    pub fn set_base_port(mut self,port: u16) -> Self {
        self.base_port = port;
        self
    }

    pub fn set_total_spu(mut self,len: usize) -> Self {
        self.total_spu = len;
        self
    }

    pub fn set_init_spu(mut self,len: usize) -> Self {
        self.init_spu = len;
        self
    }

    pub fn total_spu(&self) -> usize {
        self.total_spu
    }

    pub fn initial_spu(&self) -> usize {
        self.init_spu
    }


    pub fn create_spu_spec(&self, spu_index: u16) -> SpuSpec {

        let port = spu_index + self.base_port + 2;
        
        SpuSpec::new(self.base_id + spu_index as i32,
            EndPoint::local_end_point(port))
    }
    

    pub fn sc_config(&self) -> ScConfig {
        
        ScConfig {
            id: self.base_id,
            public_endpoint: EndPoint::local_end_point(self.base_port),
            private_endpoint: EndPoint::local_end_point(self.base_port + 1),
            run_k8_dispatchers: false,
            ..Default::default()
        }
    }


    /// create mock sc server which only run internal services.
    pub fn create_sc_server(&self) -> ((InternalApiServer,Receiver<bool>),ScClient)
    {

        let config = self.sc_config();
        
        let (private_terminator,receiver_private) = channel::<bool>(1);
        let (sc_sender,sc_receiver) = channel::<ScRequest>(100);

        let kv_store = SharedKVStore::new(sc_sender.clone());
        let (ctx,sc_request_sender,hc_sender,_dispatch_sender,private_server)  = create_core_services(config.clone(),kv_store.clone(),(sc_sender,sc_receiver));

        (
            (private_server,receiver_private),
            ScClient {
                config,
                sc_request_sender,
                private_terminator,
                hc_sender,
                ctx,
                kv_store
        })
    }

    pub fn run_spu_server<T>(&self,spec: SpuSpec, test_runner: Arc<ScTestRunner<T>>,receiver: Receiver<bool>) -> SharedSpuContext 
        where T: ScTest + Sync + Send + 'static
    {
        let spu_contxt = SpuGlobalContext::new_shared_context(spec);
        spu_contxt.clone().run( test_runner,receiver);
        spu_contxt
    }

    pub fn run_server_with_index<T>(&self,i: usize,test_runner: Arc<ScTestRunner<T>>) -> (SharedSpuContext,Sender<bool>) 
        where T: ScTest + Sync + Send + 'static
    {
        let spu_spec = self.create_spu_spec(i as u16);
        let (sender, receiver) = channel::<bool>(1);
        let ctx = self.run_spu_server(spu_spec, test_runner, receiver);
        (ctx,sender)
    }
 
}


/// Client representation to ScServer
pub struct ScClient{
    config: ScConfig,
    ctx: SharedScMetadata,
    private_terminator: Sender<bool>,
    sc_request_sender: ScRequestSender,
    hc_sender: HcActionSender,
    kv_store: SharedKVStore
}

impl ScClient {


    pub fn config(&self) -> &ScConfig {
        &self.config
    }

    pub fn kv_store(&self) -> SharedKVStore {
        self.kv_store.clone()
    }


    pub async fn terminate_private_server(&self)  {
        debug!("terminating sc private server");
        let mut terminate = self.private_terminator.clone();
        terminate
            .send(true)
            .await
            .expect("sc private shutdown should work");
    }

     #[allow(dead_code)]
    pub async fn send_to_internal_server<'a,R>(&'a self, req_msg: &'a RequestMessage<R>) -> Result<(), FlvSocketError> where R: Request,
    {
        
        let end_point = &self.config().private_endpoint;
        debug!(
            "client: trying to connect to private endpoint: {}",
            end_point
        );
        let mut socket = FlvSocket::connect(&end_point.addr).await?;
        debug!("connected to internal endpoint {}", end_point);
        let res_msg = socket.send(&req_msg).await?;
        debug!("response: {:#?}", res_msg);
        Ok(())
    }

    #[allow(dead_code)]
    pub async fn send_to_public_server<'a,R>(&'a self, req_msg: &'a RequestMessage<R>) -> Result<ResponseMessage<R::Response>, FlvSocketError>
    where
        R: Request,
    {
        let end_point = &self.config().public_endpoint;

        debug!(
            "client: trying to connect to public endpoint: {}",
            end_point
        );

        let mut socket = FlvSocket::connect(&end_point.addr).await?;
        debug!("connected to public end point {:#?}", end_point);
        let res_msg = socket.send(&req_msg).await?;
        debug!("response: {:#?}", res_msg);
        Ok(res_msg)
    }

    pub async fn send_sc_request(&self,request: ScRequest) -> Result<(),ServerError> 
    {
        let mut sender = self.sc_request_sender.clone();
        sender.send(request).await.map_err(|err| err.into())
    }

    pub async fn send_hc_action(&self,actions: Actions<HcAction>) -> Result<(),ServerError> 
    {
        let mut sender = self.hc_sender.clone();
        sender.send(actions).await.map_err(|err| err.into())
    }


}

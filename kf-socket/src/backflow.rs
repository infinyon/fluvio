use std::fmt::Debug;
use std::sync::Arc;
use std::hash::Hash;
use std::net::ToSocketAddrs;
use std::io::Error as IoError;

use log::trace;
use log::error;
use futures::stream::StreamExt;
use futures::Future;

use future_helper::spawn;
use kf_protocol::Decoder as KfDecoder;
use kf_protocol::api::RequestMessage;


use crate::SocketPool;
use crate::KfSocketError;

/// perform backflow handling
/// backflow is where you respond to msg from server
#[allow(dead_code)]
pub async fn back_flow<R,T,Fut,F>(pool: &SocketPool<T>, id: T, handler: F) -> Result<(),KfSocketError> 
    where   
        F: Fn(RequestMessage<R>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output=Result<(),IoError>> + Send,
        T: Eq + Hash + Debug + Clone + ToSocketAddrs,
        R: Send + 'static,
        RequestMessage<R>: KfDecoder + Default + Debug,        
{

    trace!("starting proces stream: {:#?}",id);
    
    if let Some(mut client) = pool.get_socket(&id) {

        {
            // mutation borrow occurs here so we need to nest
            let mut req_stream  = client.get_mut_stream().request_stream();
            let c_handler = Arc::new(handler);
            while let Some(item) = req_stream.next().await {
                let msg: RequestMessage<R> = item?;
                trace!("processing new msg: {:#?}",msg);
                let s_handler = c_handler.clone();
                spawn(async move {
                    match s_handler(msg).await {
                        Err(err) =>  error!("error handling: {}",err),
                        _ => {}
                    }
                })
            }
        }
       

        client.set_stale();
        
    }
    
    

    Ok(())
}



/*
#[cfg(test)]
mod test {

    use std::net::SocketAddr;
    use std::time::Duration;
    use std::io::Error as IoError;
    use std::fmt::Debug;

    use log::debug;
    use log::error;
    use futures::stream::StreamExt;
    use futures::future::FutureExt;

    use future_helper::test_async;
    use future_helper::sleep;
    use future_aio::net::AsyncTcpListener;

    use kf_protocol::api::RequestMessage;
    use crate::ClientPooling;
    use crate::KfSocket;
    use crate::KfSocketError;
    use crate::test_request::TestApiRequest;
    use crate::test_request::EchoRequest;
    use crate::test_request::TestKafkaApiEnum;
    use crate::pooling::test::server_loop;
    
    use super::back_flow;

    type TestPooling = ClientPooling<String>;


    /// create server and 
    async fn create_server(addr: String,_client_count: u16) -> Result<(),KfSocketError> {

        let socket_addr = addr.parse::<SocketAddr>().expect("parse");
        {
            server_loop(&socket_addr,0).await?;
        }
        Ok(())
    }

    async fn client_check(client_pool: &TestPooling,addr: String,id: u16) -> Result<(),KfSocketError> 
        where RequestMessage<TestApiRequest,TestKafkaApiEnum>: Debug
    {

        debug!("client: {}-{} client start: sleeping for 100 second to give server chances",&addr,id);
        sleep(Duration::from_millis(10)).await.expect("panic");
        back_flow(client_pool,addr,
            |req: RequestMessage<TestApiRequest,TestKafkaApiEnum> |  {

                async move {

                    debug!("client: {}-{} message from server: {:#?}",&addr,id,req);

                    match req.request {
                        TestApiRequest::EchoRequest(ech_req) => {
                            debug!("client: {}-{} message {} from server",&addr,id,ech_req.msg);
                            assert_eq!(ech_req.msg,"Hello");
                        },
                        _ => assert!(false,"not echo")
                    }

                    Ok(()) as Result<(),IoError>

                }            
            }).await;
        
        Ok(())

    }

    async fn test_client(client_pool: &TestPooling, addr: String)  -> Result<(),KfSocketError> {

        client_check(client_pool,addr.clone(),0).await.expect("should finished");
        debug!("client wait for 1 second for 2nd server to come up");
        sleep(Duration::from_millis(1000)).await.expect("panic");
        client_check(client_pool,addr.clone(),1).await.expect("should be finished");
        Ok(())
    }
    

    #[test_async]
    async fn test_backflow() -> Result<(),KfSocketError> {

        utils::init_logger();

        let count = 1;

       
        let addr1 = "127.0.0.1:20001".to_owned();
     
        let client_pool = TestPooling::new();
        let server_ft1 = create_server(addr1.clone(),count);
        let client_ft1 = test_client(&client_pool,addr1);
    
        client_ft1.join(server_ft1).await;

        Ok(())


    }

}
*/
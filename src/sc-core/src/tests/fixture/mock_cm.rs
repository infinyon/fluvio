/// mock connection mananger
/// 

use std::sync::Arc;

use log::debug;
use futures::future::BoxFuture;
use futures::future::FutureExt;

use error::ServerError;
use kf_protocol::api::Request;
use kf_protocol::api::RequestMessage;
use flv_types::SpuId;
use utils::actions::Actions;


use crate::conn_manager::SpuConnections;
use crate::conn_manager::ConnAction;

// -----------------------------------
// Data Structures
// -----------------------------------
pub type SharedMockConnManager = Arc<MockConnectionManager>;


pub struct MockConnectionManager {

}

impl MockConnectionManager {
    pub fn shared_conn_manager() -> Arc<Self> {
        Arc::new(MockConnectionManager{})
    }
}



impl SpuConnections for MockConnectionManager {

    type ResponseFuture = BoxFuture<'static, Result<(),ServerError>>;

    /// send request message to specific spu
    /// this is a one way send
    fn send_msg<R>(self: Arc<Self>, _spu_id: SpuId, _req_msg: RequestMessage<R>) -> Self::ResponseFuture 
        where R: Request + Send + Sync + 'static
    {
        async move {
            Ok(())
        }.boxed()
        
    }


    // -----------------------------------
    // Action Request Processing
    // -----------------------------------

    /// process connection action request
    fn process_connection_request(&self, actions: Actions<ConnAction>) {
        debug!("conn actions: {:?}", actions);
    }

    // -----------------------------------
    // Fromatting
    // -----------------------------------

    /// return connection information in table format
    //#[cfg(test)]
    fn table_fmt(&self) -> String {
        String::new()
    }


}


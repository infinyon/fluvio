use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

use std::sync::RwLock;

use chashmap::CHashMap;
use chashmap::WriteGuard;
use tracing::trace;

use crate::FlvSocket;
use crate::FlvSocketError;

/// pooling of sockets
#[derive(Debug)]
pub struct SocketPool<T>
where
    T: Eq + Hash,
{
    clients: CHashMap<T, FlvSocket>,
    ids: RwLock<HashMap<T, bool>>,
}

impl<T> SocketPool<T>
where
    T: Eq + PartialEq + Hash + Debug + Clone,
    FlvSocket: Sync,
{
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            clients: CHashMap::new(),
            ids: RwLock::new(HashMap::new()),
        }
    }

    pub fn insert_socket(&self, id: T, socket: FlvSocket) {
        trace!("inserting connection: {:#?}, returning", id);
        let mut ids = self.ids.write().expect("id lock must always lock");
        ids.insert(id.clone(), true);
        self.clients.insert(id, socket);
    }

    /// get valid client.  return only client which is not stale
    pub fn get_socket(&self, id: &T) -> Option<WriteGuard<'_, T, FlvSocket>> {
        if let Some(client) = self.clients.get_mut(id) {
            trace!("got existing connection: {:#?}, returning", id);
            if client.is_stale() {
                trace!("connection is stale, do not return");
                None
            } else {
                Some(client)
            }
        } else {
            trace!("no existing connection: {:#?}, returning", id);
            None
        }
    }
}

impl<T> Default for SocketPool<T>
where
    T: Eq + PartialEq + Hash + Debug + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> SocketPool<T>
where
    T: Eq + PartialEq + Hash + Debug + Clone + ToString,
    FlvSocket: Sync,
{
    /// make connection where id can be used as address
    pub async fn make_connection(&self, id: T) -> Result<(), FlvSocketError> {
        let addr = id.to_string();
        self.make_connection_with_addr(id, &addr).await
    }
}

impl<T> SocketPool<T>
where
    T: Eq + PartialEq + Hash + Debug + Clone,
    FlvSocket: Sync,
{
    /// make connection with addres as separate parameter
    pub async fn make_connection_with_addr(&self, id: T, addr: &str) -> Result<(), FlvSocketError> {
        trace!("creating new connection: {:#?}", addr);
        let client = FlvSocket::connect(addr).await?;
        trace!("got connection to server: {:#?}", &id);
        self.insert_socket(id.clone(), client);
        trace!("finish connection to server: {:#?}", &id);
        Ok(())
    }

    /// get existing socket connection or make new one
    pub async fn get_or_make<'a>(
        &'a self,
        id: T,
        addr: &'a str,
    ) -> Result<Option<WriteGuard<'a, T, FlvSocket>>, FlvSocketError> {
        if let Some(socket) = self.get_socket(&id) {
            return Ok(Some(socket));
        }

        self.make_connection_with_addr(id.clone(), addr).await?;

        Ok(self.get_socket(&id))
    }
}

#[cfg(test)]
pub(crate) mod test {

    use std::net::SocketAddr;
    use std::time::Duration;

    use futures_util::future::join;
    use futures_util::stream::StreamExt;
    use tracing::debug;
    use tracing::error;

    use fluvio_future::net::TcpListener;
    use fluvio_future::test_async;
    use fluvio_future::timer::sleep;

    use super::FlvSocket;
    use super::FlvSocketError;
    use super::SocketPool;
    use crate::test_request::EchoRequest;
    use fluvio_protocol::api::RequestMessage;

    type TestPooling = SocketPool<String>;

    pub(crate) async fn server_loop(
        socket_addr: &SocketAddr,
        id: u16,
    ) -> Result<(), FlvSocketError> {
        debug!("server: {}-{} ready to bind", socket_addr, id);
        let listener = TcpListener::bind(&socket_addr).await?;
        debug!(
            "server: {}-{} successfully binding. waiting for incoming",
            socket_addr, id
        );
        let mut incoming = listener.incoming();
        if let Some(stream) = incoming.next().await {
            debug!(
                "server: {}-{} got connection from client, sending rely",
                socket_addr, id
            );

            let stream = stream?;
            let mut socket: FlvSocket = stream.into();

            let msg: RequestMessage<EchoRequest> = RequestMessage::new_request(EchoRequest {
                msg: "Hello".to_owned(),
            });

            socket.get_mut_sink().send_request(&msg).await?;
            debug!("server: {}-{} finish send echo", socket_addr, id);
        } else {
            error!("no content from client");
        }

        // server terminating
        drop(incoming);
        debug!(
            "server: {}-{} sleeping for 100ms  to give client chances",
            socket_addr, id
        );
        debug!("server: {}-{} server loop ended", socket_addr, id);
        Ok(())
    }

    /// create server and
    async fn create_server(addr: String, _client_count: u16) -> Result<(), FlvSocketError> {
        let socket_addr = addr.parse::<SocketAddr>().expect("parse");

        {
            server_loop(&socket_addr, 0).await?;
        }
        {
            server_loop(&socket_addr, 1).await?;
        }

        Ok(())
    }

    async fn client_check(
        client_pool: &TestPooling,
        addr: String,
        id: u16,
    ) -> Result<(), FlvSocketError> {
        debug!(
            "client: {}-{} client start: sleeping for 100 second to give server chances",
            &addr, id
        );
        sleep(Duration::from_millis(10)).await;
        debug!("client: {}-{} trying to connect to server", &addr, id);
        client_pool.make_connection(addr.clone()).await?;

        if let Some(mut client_socket) = client_pool.get_socket(&addr) {
            debug!("client: {}-{} got socket from server", &addr, id);
            // create new scope, so we limit mut borrow
            {
                let mut req_stream = client_socket.get_mut_stream().request_stream();
                debug!(
                    "client: {}-{} waiting for echo request from server",
                    &addr, id
                );
                let next = req_stream.next().await;
                if let Some(result) = next {
                    let req_msg: RequestMessage<EchoRequest> = result?;
                    debug!(
                        "client: {}-{} message {} from server",
                        &addr, id, req_msg.request.msg
                    );
                    assert_eq!(req_msg.request.msg, "Hello");

                    // await for next
                    debug!(
                        "client: {}-{} wait for 2nd, server should terminate this point",
                        &addr, id
                    );
                    let next2 = req_stream.next().await;
                    assert!(next2.is_none(), "next2 should be none");
                    debug!("client: {}-{} 2nd wait finished", &addr, id);
                }
            }

            debug!("client: {}-{} mark as stale", &addr, id);
            client_socket.set_stale();
            Ok(())
        } else {
            panic!("not able to connect: {}", addr);
        }
    }

    async fn test_client(client_pool: &TestPooling, addr: String) -> Result<(), FlvSocketError> {
        client_check(client_pool, addr.clone(), 0)
            .await
            .expect("should finished");
        debug!("client wait for 1 second for 2nd server to come up");
        sleep(Duration::from_millis(1000)).await;
        client_check(client_pool, addr.clone(), 1)
            .await
            .expect("should be finished");
        Ok(())
    }
    #[test_async]
    async fn test_pool() -> Result<(), FlvSocketError> {
        let count = 1;

        // create fake server, anything will do since we only
        // care creating tcp stream
        let addr1 = "127.0.0.1:20001".to_owned();
        let addr2 = "127.0.0.1:20002".to_owned();

        let server_ft1 = create_server(addr1.clone(), count);
        let server_ft2 = create_server(addr2.clone(), count);

        let client_pool = TestPooling::new();
        let client_ft1 = test_client(&client_pool, addr1);
        let client_ft2 = test_client(&client_pool, addr2);

        let _fr = join(join(client_ft1, client_ft2), join(server_ft1, server_ft2)).await;
        Ok(())
    }
}

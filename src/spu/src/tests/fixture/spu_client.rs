use std::convert::TryInto;
use std::net::SocketAddr;

use tracing::debug;
use futures::channel::mpsc::Sender;

use kf_socket::KfSocketError;
use kf_socket::KfSocket;
use kf_protocol::api::Request;
use kf_protocol::api::RequestMessage;
use kf_protocol::api::ResponseMessage;
use fluvio_types::SpuId;
use fluvio_controlplane_metadata::spu::SpuSpec;

use super::mock_sc::SharedScContext;

pub struct SpuServer(SpuSpec);

impl SpuServer {
    pub fn new(spec: SpuSpec) -> Self {
        Self(spec)
    }

    pub fn spec(&self) -> &SpuSpec {
        &self.0
    }

    pub fn id(&self) -> SpuId {
        self.0.id
    }

    #[allow(dead_code)]
    pub async fn send_to_internal_server<'a, R>(
        &'a self,
        req_msg: &'a RequestMessage<R>,
    ) -> Result<(), KfSocketError>
    where
        R: Request,
    {
        debug!(
            "client: trying to connect to private endpoint: {:#?}",
            self.private_endpoint
        );
        let socket: SocketAddr = (&self.spec().private_endpoint).try_into()?;
        let mut socket = KfSocket::connect(&socket).await?;
        debug!(
            "connected to internal endpoint {:#?}",
            self.spec().private_endpoint
        );
        let res_msg = socket.send(&req_msg).await?;
        debug!("response: {:#?}", res_msg);
        Ok(())
    }

    pub async fn send_to_public_server<'a, R>(
        &'a self,
        req_msg: &'a RequestMessage<R>,
    ) -> Result<ResponseMessage<R::Response>, KfSocketError>
    where
        R: Request,
    {
        debug!(
            "client: trying to connect to public endpoint: {:#?}",
            self.0.public_endpoint
        );
        let socket: SocketAddr = (&self.spec().public_endpoint).try_into()?;
        let mut socket = KfSocket::connect(&socket).await?;
        debug!(
            "connected to public end point {:#?}",
            self.spec().public_endpoint
        );
        let res_msg = socket.send(&req_msg).await?;
        debug!("response: {:#?}", res_msg);
        Ok(res_msg)
    }
}

impl From<SpuSpec> for SpuServer {
    fn from(spec: SpuSpec) -> Self {
        Self::new(spec)
    }
}

struct ScServerCtx {
    ctx: SharedScContext,
    sender: Sender<bool>,
}

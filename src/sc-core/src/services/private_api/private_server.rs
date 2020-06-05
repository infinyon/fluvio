use std::sync::Arc;
use std::io::Error as IoError;
use std::io::ErrorKind;

use log::error;
use log::debug;
use async_trait::async_trait;

use flv_future_aio::net::TcpStream;
use kf_service::api_loop;
use kf_service::KfService;
use kf_service::wait_for_request;
use kf_socket::KfSocket;
use kf_socket::KfSocketError;
use internal_api::InternalScRequest;
use internal_api::InternalScKey;
use internal_api::RegisterSpuResponse;

use crate::conn_manager::ConnParams;
use super::SharedInternalContext;

pub struct ScInternalService {}

impl ScInternalService {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl KfService<TcpStream> for ScInternalService {
    type Context = SharedInternalContext;
    type Request = InternalScRequest;

    async fn respond(
        self: Arc<Self>,
        context: SharedInternalContext,
        socket: KfSocket,
    ) -> Result<(), KfSocketError> {
        let (mut sink, mut stream) = socket.split();
        let mut api_stream = stream.api_stream::<InternalScRequest, InternalScKey>();

        // wait for spu registeration request
        let spu_id = wait_for_request!(api_stream,
            InternalScRequest::RegisterSpuRequest(req_msg) => {
                let spu_id = req_msg.request.spu();
                let mut status = true;
                debug!("registration req from spu '{}'", spu_id);


                let register_res = if context.validate_spu(&spu_id) {
                    debug!("SPU: {} validation succeed",spu_id);
                    RegisterSpuResponse::ok()
                } else {
                    status = false;
                    debug!("SPU: {} validation failed",spu_id);
                    RegisterSpuResponse::failed_registeration()
                };

                let response = req_msg.new_response(register_res);
                sink.send_response(&response,req_msg.header.api_version()).await?;

                if status {
                    context.register_sink(spu_id,sink,ConnParams::new()).await;
                } else {
                    return Ok(())
                }


                spu_id
            }
        );

        api_loop!(
            api_stream,
            InternalScRequest::UpdateLrsRequest(msg) => {
                debug!("received lrs request: {}",msg);
                context.send_lrs_to_sender(msg.request).await;
            },
            InternalScRequest::RegisterSpuRequest(_request) => {
                error!("registration req only valid during initialization");
                return Err(KfSocketError::IoError(IoError::new(ErrorKind::InvalidData,"register spu request is only valid at init")))
            }
        );

        debug!("api loop terminated; clearing sink");
        context.clear_sink(&spu_id).await;

        Ok(())
    }
}

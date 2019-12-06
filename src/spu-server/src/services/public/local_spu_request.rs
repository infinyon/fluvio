use std::io::Error as IoError;

use log::trace;

use kf_protocol::api::RequestMessage;
use kf_protocol::api::ResponseMessage;

use spu_api::spus::FlvFetchLocalSpuRequest;
use spu_api::spus::FlvFetchLocalSpuResponse;
use spu_api::spus::EndPointMetadata;

use crate::core::DefaultSharedGlobalContext;

pub async fn handle_spu_request(
    req_msg: RequestMessage<FlvFetchLocalSpuRequest>,
    ctx: DefaultSharedGlobalContext,
) -> Result<ResponseMessage<FlvFetchLocalSpuResponse>, IoError> {
    let mut response = FlvFetchLocalSpuResponse::default();
    let config = ctx.config();

    response.id = config.id();
    response.managed = !config.is_custom();
    response.public_ep = EndPointMetadata {
        host: config.public_server_addr().host.clone(),
        port: config.public_server_addr().port,
    };
    response.private_ep = EndPointMetadata {
        host: config.private_server_addr().host.clone(),
        port: config.private_server_addr().port,
    };
    response.rack = config.rack().clone();
    
    trace!("fetch local spu res {:#?}", response);
    Ok(req_msg.new_response(response))
}

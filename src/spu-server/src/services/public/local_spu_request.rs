use std::io::Error as IoError;

use log::trace;

use kf_protocol::api::RequestMessage;
use kf_protocol::api::ResponseMessage;

use spu_api::spus::FlvFetchLocalSpuRequest;
use spu_api::spus::FlvFetchLocalSpuResponse;


use crate::core::DefaultSharedGlobalContext;

pub async fn handle_spu_request(
    req_msg: RequestMessage<FlvFetchLocalSpuRequest>,
    ctx: DefaultSharedGlobalContext,
) -> Result<ResponseMessage<FlvFetchLocalSpuResponse>, IoError> {
    let mut response = FlvFetchLocalSpuResponse::default();
    let config = ctx.config();

    response.id = config.id();
    response.public_ep = config.public_server_addr().to_owned();
    response.private_ep = config.private_server_addr().to_owned();
    response.rack = config.rack().clone();
    
    trace!("fetch local spu res {:#?}", response);
    Ok(req_msg.new_response(response))
}

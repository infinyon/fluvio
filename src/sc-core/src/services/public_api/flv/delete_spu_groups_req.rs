//!
//! # Delete Spu Groups Request
//!
//! Delete spu groups request handler. Lookup spu-group in local metadata, grab its K8 context
//! and send K8 a delete message.
//!
use log::{debug, trace};
use std::io::Error;

use k8_metadata::spg::SpuGroupSpec;
use k8_metadata_client::MetadataClient;
use kf_protocol::api::FlvErrorCode;
use kf_protocol::api::{RequestMessage, ResponseMessage};
use sc_api::{FlvResponseMessage};
use sc_api::spu::{FlvDeleteSpuGroupsRequest, FlvDeleteSpuGroupsResponse};

use super::PublicContext;

/// Handler for delete spu group request
pub async fn handle_delete_spu_groups_request<C>(
    request: RequestMessage<FlvDeleteSpuGroupsRequest>,
    ctx: &PublicContext<C>,
) -> Result<ResponseMessage<FlvDeleteSpuGroupsResponse>, Error>
where
    C: MetadataClient,
{
    let mut response = FlvDeleteSpuGroupsResponse::default();
    let mut results: Vec<FlvResponseMessage> = vec![];

    debug!(
        ">>>>>>>>>>> DELETE SPU GROUP REQ GOES HERE {:#?}",
        request.request
    );

    for spg_name in &request.request.spu_groups {
        debug!("api request: delete spu group '{}'", spg_name);

        let result = match ctx.delete::<SpuGroupSpec>(spg_name).await {
            Ok(_) => FlvResponseMessage::new_ok(spg_name.clone()),
            Err(err) => {
                let error = Some(err.to_string());
                FlvResponseMessage::new(spg_name.clone(), FlvErrorCode::SpuError, error)
            }
        };

        results.push(result);
    }

    // update response
    response.results = results;
    trace!("flv delete spu group resp {:#?}", response);

    Ok(request.new_response(response))
}

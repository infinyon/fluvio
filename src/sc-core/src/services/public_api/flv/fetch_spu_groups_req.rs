use log::debug;
use std::io::Error;

use kf_protocol::api::FlvErrorCode;
use kf_protocol::api::{RequestMessage, ResponseMessage};

use k8_metadata::spg::{SpuGroupSpec};
use k8_metadata_client::MetadataClient;

use sc_api::spu::{FlvFetchSpuGroupsRequest, FlvFetchSpuGroupsResponse};
use sc_api::FlvResponseMessage;

use super::PublicContext;

pub async fn handle_fetch_spu_groups_request<C>(
    request: RequestMessage<FlvFetchSpuGroupsRequest>,
    ctx: &PublicContext<C>,
) -> Result<ResponseMessage<FlvFetchSpuGroupsResponse>, Error>
where
    C: MetadataClient,
{
    let mut response = FlvFetchSpuGroupsResponse::default();

    match ctx.retrieve_items::<SpuGroupSpec>().await {
        Ok(k8_list) => {
            debug!("fetched: {} spgs", k8_list.items.len());
            for group in k8_list.items {
                response.spu_groups.push(group.into());
            }
        }
        Err(err) => {
            let error = Some(err.to_string());
            response.error =
                FlvResponseMessage::new("error".to_owned(), FlvErrorCode::SpuError, error);
        }
    }

    Ok(request.new_response(response))
}

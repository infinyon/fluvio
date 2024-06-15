mod add;

use std::sync::Arc;

use anyhow::{anyhow, Result};
use tracing::{info, instrument};
use fluvio_auth::AuthContext;
use fluvio_protocol::api::{RequestMessage, ResponseMessage};
use fluvio_stream_model::core::MetadataItem;
use fluvio_sc_schema::{
    topic::{AddPartition, TopicUpdateRequest},
    Status, TryEncodableFrom,
};
use crate::services::auth::AuthServiceContext;
use fluvio_sc_schema::topic::ObjectTopicUpdateRequest;

pub enum TopicUpdateRequests {
    AddPartition(AddPartition),
}

#[instrument(skip(request, auth_ctx))]
pub async fn handle_topic_update_request<AC: AuthContext, C: MetadataItem>(
    request: RequestMessage<ObjectTopicUpdateRequest>,
    auth_ctx: Arc<AuthServiceContext<AC, C>>,
) -> Result<ResponseMessage<Status>> {
    info!("remote cluster register request {:?}", request);

    let (header, req) = request.get_header_request();

    let Ok(req) = try_convert_to_reqs(req) else {
        return Err(anyhow!("unable to decode request"));
    };

    let status = match req {
        TopicUpdateRequests::AddPartition(req) => add::handle_add_partition(req, auth_ctx).await?,
    };

    Ok(ResponseMessage::from_header(&header, status))
}

pub fn try_convert_to_reqs(ob: ObjectTopicUpdateRequest) -> Result<TopicUpdateRequests> {
    if let Some(req) = ob.downcast()? as Option<TopicUpdateRequest<AddPartition>> {
        return Ok(TopicUpdateRequests::AddPartition(req.request));
    }

    Err(anyhow!("Invalid TopicUpdate Request"))
}

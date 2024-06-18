mod add;

use anyhow::Result;
use tracing::{info, instrument};
use fluvio_auth::AuthContext;
use fluvio_stream_model::core::MetadataItem;
use fluvio_sc_schema::{topic::UpdateTopicAction, Status};
use crate::services::auth::AuthServiceContext;

#[instrument(skip(topic_name, action, auth_ctx))]
pub async fn handle_topic_update_request<AC: AuthContext, C: MetadataItem>(
    topic_name: String,
    action: UpdateTopicAction,
    auth_ctx: &AuthServiceContext<AC, C>,
) -> Result<Status> {
    info!("handling topic update request");

    let status = match action {
        UpdateTopicAction::AddPartition(req) => {
            add::handle_add_partition(topic_name, req, auth_ctx).await?
        }
    };

    Ok(status)
}

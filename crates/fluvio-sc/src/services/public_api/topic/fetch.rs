use fluvio_stream_model::core::MetadataItem;
use tracing::{trace, debug, instrument};
use anyhow::{anyhow, Result};

use fluvio_controlplane_metadata::store::KeyFilter;
use fluvio_sc_schema::objects::{ListResponse, Metadata, ListFilters};
use fluvio_sc_schema::topic::TopicSpec;
use fluvio_auth::{AuthContext, TypeAction};
use fluvio_controlplane_metadata::extended::SpecExt;

use crate::services::auth::AuthServiceContext;

#[instrument(skip(filters, auth_ctx))]
pub async fn handle_fetch_topics_request<AC: AuthContext, C: MetadataItem>(
    filters: ListFilters,
    system: bool,
    auth_ctx: &AuthServiceContext<AC, C>,
) -> Result<ListResponse<TopicSpec>> {
    debug!("retrieving topic list: {:#?}", filters);

    if let Ok(authorized) = auth_ctx
        .auth
        .allow_type_action(TopicSpec::OBJECT_TYPE, TypeAction::Read)
        .await
    {
        if !authorized {
            trace!("authorization failed");
            return Ok(ListResponse::new(vec![]));
        }
    } else {
        return Err(anyhow!("authorization error"));
    }

    let topics: Vec<Metadata<TopicSpec>> = auth_ctx
        .global_ctx
        .topics()
        .store()
        .read()
        .await
        .values()
        .filter(|value| value.inner().spec().is_system() == system)
        .filter_map(|value| {
            if filters.filter(value.key()) {
                Some(value.inner().clone().into())
            } else {
                None
            }
        })
        .collect();

    debug!("flv fetch topics resp: {} items", topics.len());
    trace!("flv fetch topics resp {:#?}", topics);

    Ok(ListResponse::new(topics))
}

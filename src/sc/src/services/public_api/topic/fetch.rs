use tracing::{trace, debug};
use std::io::{Error, ErrorKind};

use fluvio_controlplane_metadata::store::KeyFilter;
use fluvio_sc_schema::objects::*;
use fluvio_sc_schema::topic::TopicSpec;
use fluvio_service::auth::Authorization;

use crate::core::AuthenticatedContext;
use crate::services::auth::basic::{Action, Object};

pub async fn handle_fetch_topics_request(
    filters: Vec<String>,
    auth_ctx: &AuthenticatedContext,
) -> Result<ListResponse, Error> {
    debug!("retrieving topic list: {:#?}", filters);

    let auth_request = (Action::Read, Object::Topic, None);
    if let Ok(authorized) = auth_ctx.auth.enforce(auth_request).await {
        if !authorized {
            trace!("authorization failed");
            return Ok(ListResponse::Topic(vec![]));
        }
    } else {
        return Err(Error::new(ErrorKind::Interrupted, "authorization io error"));
    }

    let topics: Vec<Metadata<TopicSpec>> = auth_ctx
        .global_ctx
        .topics()
        .store()
        .read()
        .await
        .values()
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

    Ok(ListResponse::Topic(topics))
}

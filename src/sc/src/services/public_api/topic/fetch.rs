use tracing::{trace, debug};
use std::io::{Error, ErrorKind};

use fluvio_controlplane_metadata::store::KeyFilter;
use fluvio_sc_schema::objects:: { ListResponse,Metadata };
use fluvio_sc_schema::topic::TopicSpec;


use crate::services::auth::AuthServiceContext;


pub async fn handle_fetch_topics_request(
    filters: Vec<String>,
    auth_ctx: &AuthServiceContext,
) -> Result<ListResponse, Error> {
    debug!("retrieving topic list: {:#?}", filters);

    
    if let Ok(authorized) = auth_ctx.auth.read::<TopicSpec>().await {
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

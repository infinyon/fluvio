use tracing::{trace, debug};
use std::io::Error;

use fluvio_controlplane_metadata::store::KeyFilter;
use fluvio_sc_schema::objects::*;
use fluvio_sc_schema::topic::TopicSpec;

use crate::core::Context;

pub async fn handle_fetch_topics_request(
    filters: Vec<String>,
    ctx: &Context,
) -> Result<ListResponse, Error> {
    debug!("retrieving topic list: {:#?}", filters);
    let topics: Vec<Metadata<TopicSpec>> = ctx
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

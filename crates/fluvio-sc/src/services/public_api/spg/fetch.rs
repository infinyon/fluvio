use std::io::{Error, ErrorKind};

use tracing::{debug, trace, instrument};

use fluvio_sc_schema::objects::{ListResponse, Metadata, ListFilters};
use fluvio_sc_schema::spg::SpuGroupSpec;
use fluvio_auth::{AuthContext, TypeAction};
use fluvio_controlplane_metadata::store::KeyFilter;
use fluvio_controlplane_metadata::extended::SpecExt;

use crate::services::auth::AuthServiceContext;

#[instrument(skip(filters, auth_ctx))]
pub async fn handle_fetch_spu_groups_request<AC: AuthContext>(
    filters: ListFilters,
    auth_ctx: &AuthServiceContext<AC>,
) -> Result<ListResponse<SpuGroupSpec>, Error> {
    debug!("fetching spu groups");

    if let Ok(authorized) = auth_ctx
        .auth
        .allow_type_action(SpuGroupSpec::OBJECT_TYPE, TypeAction::Read)
        .await
    {
        if !authorized {
            trace!("authorization failed");
            // If permission denied, return empty list;
            return Ok(ListResponse::new(vec![]));
        }
    } else {
        return Err(Error::new(ErrorKind::Interrupted, "authorization io error"));
    }

    let spgs: Vec<Metadata<SpuGroupSpec>> = auth_ctx
        .global_ctx
        .spgs()
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

    debug!("flv fetch spgs resp: {} items", spgs.len());
    trace!("flv fetch spgs resp {:#?}", spgs);

    Ok(ListResponse::new(spgs))
}

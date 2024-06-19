use fluvio_controlplane_metadata::{
    spu::{CustomSpuSpec, SpuSpec},
    topic::TopicSpec,
    spg::SpuGroupSpec,
    partition::PartitionSpec,
    smartmodule::SmartModuleSpec,
    tableformat::TableFormatSpec,
};
use fluvio_stream_model::core::MetadataItem;
use tracing::{debug, instrument};
use anyhow::Result;

use fluvio_protocol::api::{RequestMessage, ResponseMessage};
use fluvio_sc_schema::{
    objects::{ListRequest, ObjectApiListRequest, ObjectApiListResponse},
    mirror::MirrorSpec,
    TryEncodableFrom,
};
use fluvio_auth::AuthContext;

use crate::services::{auth::AuthServiceContext, public_api::mirror::handle_list_mirror};
use super::smartmodule::fetch_smart_modules;

#[instrument(skip(request, auth_ctx))]
pub async fn handle_list_request<AC: AuthContext, C: MetadataItem>(
    request: RequestMessage<ObjectApiListRequest>,
    auth_ctx: &AuthServiceContext<AC, C>,
) -> Result<ResponseMessage<ObjectApiListResponse>> {
    let (header, req) = request.get_header_request();
    debug!("list header: {:#?}, request: {:#?}", header, req);

    let response = if let Some(req) = req.downcast()? as Option<ListRequest<TopicSpec>> {
        ObjectApiListResponse::try_encode_from(
            super::topic::handle_fetch_topics_request(req.name_filters, req.system, auth_ctx)
                .await?,
            header.api_version(),
        )?
    } else if let Some(req) = req.downcast()? as Option<ListRequest<SpuSpec>> {
        ObjectApiListResponse::try_encode_from(
            super::spu::handle_fetch_spus_request(req.name_filters, auth_ctx).await?,
            header.api_version(),
        )?
    } else if let Some(req) = req.downcast()? as Option<ListRequest<SpuGroupSpec>> {
        ObjectApiListResponse::try_encode_from(
            super::spg::handle_fetch_spu_groups_request(req.name_filters, auth_ctx).await?,
            header.api_version(),
        )?
    } else if let Some(req) = req.downcast()? as Option<ListRequest<CustomSpuSpec>> {
        ObjectApiListResponse::try_encode_from(
            super::spu::handle_fetch_custom_spu_request(req.name_filters, auth_ctx).await?,
            header.api_version(),
        )?
    } else if let Some(req) = req.downcast()? as Option<ListRequest<PartitionSpec>> {
        ObjectApiListResponse::try_encode_from(
            super::partition::handle_fetch_request(req.name_filters, req.system, auth_ctx).await?,
            header.api_version(),
        )?
    } else if let Some(req) = req.downcast()? as Option<ListRequest<SmartModuleSpec>> {
        ObjectApiListResponse::try_encode_from(
            fetch_smart_modules(
                req.name_filters.into(),
                req.summary,
                &auth_ctx.auth,
                auth_ctx.global_ctx.smartmodules(),
            )
            .await?,
            header.api_version(),
        )?
    } else if let Some(req) = req.downcast()? as Option<ListRequest<TableFormatSpec>> {
        ObjectApiListResponse::try_encode_from(
            fetch::handle_fetch_request(
                req.name_filters,
                auth_ctx,
                auth_ctx.global_ctx.tableformats(),
            )
            .await?,
            header.api_version(),
        )?
    } else if let Some(req) = req.downcast()? as Option<ListRequest<MirrorSpec>> {
        ObjectApiListResponse::try_encode_from(
            handle_list_mirror(req.name_filters, auth_ctx).await?,
            header.api_version(),
        )?
    } else {
        return Err(anyhow::anyhow!("unsupported list request: {:#?}", req));
    };

    debug!("response: {:#?}", response);

    Ok(ResponseMessage::from_header(&header, response))
}

mod fetch {

    use std::io::{Error, ErrorKind};

    use fluvio_controlplane_metadata::core::Spec;
    use fluvio_protocol::{Decoder, Encoder};
    use fluvio_sc_schema::AdminSpec;
    use fluvio_stream_dispatcher::store::StoreContext;
    use fluvio_stream_model::core::MetadataItem;
    use tracing::{debug, trace, instrument};

    use fluvio_sc_schema::objects::{ListResponse, Metadata, ListFilters};
    use fluvio_auth::{AuthContext, TypeAction};
    use fluvio_controlplane_metadata::store::MetadataStoreObject;
    use fluvio_controlplane_metadata::extended::SpecExt;
    use fluvio_controlplane_metadata::store::KeyFilter;

    use crate::services::auth::AuthServiceContext;

    #[instrument(skip(filters, auth_ctx))]
    pub async fn handle_fetch_request<AC, C: MetadataItem, S>(
        filters: ListFilters,
        auth_ctx: &AuthServiceContext<AC, C>,
        object_ctx: &StoreContext<S, C>,
    ) -> Result<ListResponse<S>, Error>
    where
        AC: AuthContext,
        S: AdminSpec + SpecExt,
        <S as Spec>::Status: Encoder + Decoder,
        <S as Spec>::IndexKey: AsRef<str>,
        Metadata<S>: From<MetadataStoreObject<S, C>>,
    {
        debug!(ty = %S::LABEL,"fetching");

        if let Ok(authorized) = auth_ctx
            .auth
            .allow_type_action(S::OBJECT_TYPE, TypeAction::Read)
            .await
        {
            if !authorized {
                debug!(ty = %S::LABEL, "authorization failed");
                // If permission denied, return empty list;
                return Ok(ListResponse::new(vec![]));
            }
        } else {
            return Err(Error::new(ErrorKind::Interrupted, "authorization io error"));
        }

        let reader = object_ctx.store().read().await;
        let objects: Vec<Metadata<S>> = reader
            .values()
            .filter_map(|value| {
                if filters.filter(value.key().as_ref()) {
                    let list_obj: Metadata<S> = AdminSpec::convert_from(value);
                    Some(list_obj)
                } else {
                    None
                }
            })
            .collect();

        debug!(fetch_items = objects.len(),);
        trace!("fetch {:#?}", objects);

        Ok(ListResponse::new(objects))
    }
}

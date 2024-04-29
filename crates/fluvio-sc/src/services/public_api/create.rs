use fluvio_protocol::link::ErrorCode;
use fluvio_sc_schema::mirror::MirrorSpec;
use fluvio_stream_model::core::MetadataItem;
use tracing::{instrument, debug, error};
use anyhow::Result;

use fluvio_controlplane_metadata::smartmodule::SmartModuleSpec;
use fluvio_controlplane_metadata::spg::SpuGroupSpec;
use fluvio_controlplane_metadata::spu::CustomSpuSpec;
use fluvio_controlplane_metadata::tableformat::TableFormatSpec;
use fluvio_controlplane_metadata::topic::TopicSpec;
use fluvio_protocol::api::{RequestMessage, ResponseMessage};
use fluvio_sc_schema::{Status, TryEncodableFrom};
use fluvio_sc_schema::objects::{ObjectApiCreateRequest, CreateRequest};
use fluvio_auth::AuthContext;

use crate::services::auth::AuthServiceContext;

/// Handler for create topic request
#[instrument(skip(request, auth_context))]
pub async fn handle_create_request<AC: AuthContext, C: MetadataItem>(
    request: Box<RequestMessage<ObjectApiCreateRequest>>,
    auth_context: &AuthServiceContext<AC, C>,
) -> Result<ResponseMessage<Status>> {
    let (header, req) = request.get_header_request();

    debug!(?req, "create request");
    let status = if let Some(create) = req.downcast()? as Option<CreateRequest<TopicSpec>> {
        super::topic::handle_create_topics_request(create, auth_context).await?
    } else if let Some(create) = req.downcast()? as Option<CreateRequest<SpuGroupSpec>> {
        super::spg::handle_create_spu_group_request(create, auth_context).await?
    } else if let Some(create) = req.downcast()? as Option<CreateRequest<CustomSpuSpec>> {
        super::spu::RegisterCustomSpu::handle_register_custom_spu_request(create, auth_context)
            .await
    } else if let Some(create) = req.downcast()? as Option<CreateRequest<SmartModuleSpec>> {
        super::smartmodule::handle_create_smartmodule_request(create, auth_context).await?
    } else if let Some(create) = req.downcast()? as Option<CreateRequest<TableFormatSpec>> {
        super::tableformat::handle_create_tableformat_request(create, auth_context).await?
    } else if let Some(create) = req.downcast()? as Option<CreateRequest<MirrorSpec>> {
        super::mirror::handle_register_mirror(create, auth_context).await?
    } else {
        error!("unknown create request: {:#?}", req);
        Status::new(
            "create error".to_owned(),
            ErrorCode::Other("unknown admin object type".to_owned()),
            None,
        )
    };

    Ok(ResponseMessage::from_header(&header, status))
}

mod create_handler {
    use std::convert::{TryFrom, TryInto};
    use std::fmt::Display;
    use std::io::{Error, ErrorKind};

    use fluvio_controlplane_metadata::core::Spec;
    use fluvio_stream_dispatcher::store::StoreContext;
    use fluvio_stream_model::core::MetadataItem;
    use tracing::{info, trace, instrument};

    use fluvio_protocol::link::ErrorCode;
    use fluvio_sc_schema::{AdminSpec, Status};
    use fluvio_sc_schema::objects::CommonCreateRequest;
    use fluvio_controlplane_metadata::extended::SpecExt;
    use fluvio_auth::{AuthContext, TypeAction};

    use crate::services::auth::AuthServiceContext;

    #[instrument(skip(create, spec, auth_ctx, object_ctx, error_code))]
    pub async fn process<AC: AuthContext, S, F, C: MetadataItem>(
        create: CommonCreateRequest,
        spec: S,
        auth_ctx: &AuthServiceContext<AC, C>,
        object_ctx: &StoreContext<S, C>,
        error_code: F,
    ) -> Result<Status, Error>
    where
        S: AdminSpec + SpecExt,
        <S as Spec>::IndexKey: TryFrom<String> + Display,
        F: FnOnce(Error) -> ErrorCode,
    {
        let name = create.name;

        info!(%name, ty = %S::LABEL,"creating");

        if let Ok(authorized) = auth_ctx
            .auth
            .allow_type_action(S::OBJECT_TYPE, TypeAction::Create)
            .await
        {
            if !authorized {
                trace!("authorization failed");
                return Ok(Status::new(
                    name.clone(),
                    ErrorCode::PermissionDenied,
                    Some(String::from("permission denied")),
                ));
            }
        } else {
            return Err(Error::new(ErrorKind::Interrupted, "authorization io error"));
        }

        Ok(
            if let Err(err) = object_ctx
                .create_spec(
                    name.clone()
                        .try_into()
                        .map_err(|_err| Error::new(ErrorKind::InvalidData, "not convertible"))?,
                    spec,
                )
                .await
            {
                let error = Some(err.to_string());
                Status::new(name, error_code(err), error)
            } else {
                info!(%name, ty = %S::LABEL,"created");

                Status::new_ok(name.clone())
            },
        )
    }
}

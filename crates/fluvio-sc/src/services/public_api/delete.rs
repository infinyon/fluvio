//!
//! # Delete Topic Request
//!
//! Delete topic request handler. Lookup topic in local metadata, grab its K8 context
//! and send K8 a delete message.
//!

use fluvio_protocol::link::ErrorCode;
use fluvio_sc_schema::mirror::MirrorSpec;
use fluvio_stream_model::core::MetadataItem;
use tracing::{instrument, trace, debug, error};
use anyhow::Result;

use fluvio_controlplane_metadata::smartmodule::SmartModuleSpec;
use fluvio_controlplane_metadata::spg::SpuGroupSpec;
use fluvio_controlplane_metadata::spu::CustomSpuSpec;
use fluvio_controlplane_metadata::tableformat::TableFormatSpec;
use fluvio_controlplane_metadata::topic::TopicSpec;
use fluvio_protocol::api::{RequestMessage, ResponseMessage};
use fluvio_sc_schema::{Status, TryEncodableFrom};
use fluvio_sc_schema::objects::{ObjectApiDeleteRequest, DeleteRequest};
use fluvio_auth::AuthContext;

use crate::services::auth::AuthServiceContext;

/// Handler for delete topic request
#[instrument(skip(request, auth_ctx))]
pub async fn handle_delete_request<AC: AuthContext, C: MetadataItem>(
    request: RequestMessage<ObjectApiDeleteRequest>,
    auth_ctx: &AuthServiceContext<AC, C>,
) -> Result<ResponseMessage<Status>> {
    let (header, del_req) = request.get_header_request();

    debug!(?del_req, "del request");

    let status = if let Some(req) = del_req.downcast()? as Option<DeleteRequest<TopicSpec>> {
        let force = req.is_force();
        super::topic::handle_delete_topic(req.key(), force, auth_ctx).await?
    } else if let Some(req) = del_req.downcast()? as Option<DeleteRequest<CustomSpuSpec>> {
        super::spu::handle_un_register_custom_spu_request(req.key(), auth_ctx).await?
    } else if let Some(req) = del_req.downcast()? as Option<DeleteRequest<SpuGroupSpec>> {
        super::spg::handle_delete_spu_group(req.key(), auth_ctx).await?
    } else if let Some(req) = del_req.downcast()? as Option<DeleteRequest<SmartModuleSpec>> {
        super::smartmodule::handle_delete_smartmodule(req.key(), auth_ctx).await?
    } else if let Some(req) = del_req.downcast()? as Option<DeleteRequest<TableFormatSpec>> {
        super::tableformat::handle_delete_tableformat(req.key(), auth_ctx).await?
    } else if let Some(req) = del_req.downcast()? as Option<DeleteRequest<MirrorSpec>> {
        super::mirror::handle_unregister_mirror(req.key(), auth_ctx).await?
    } else {
        error!("unknown create request: {:#?}", del_req);
        Status::new(
            "create error".to_owned(),
            ErrorCode::Other("unknown admin object type".to_owned()),
            None,
        )
    };

    trace!("flv delete topics resp {:#?}", status);

    Ok(ResponseMessage::from_header(&header, status))
}

mod delete_handler {
    use std::{
        convert::{TryFrom, TryInto},
        io::{Error, ErrorKind},
    };

    use fluvio_protocol::link::ErrorCode;
    use fluvio_stream_dispatcher::store::StoreContext;
    use fluvio_stream_model::core::MetadataItem;
    use tracing::{info, trace, instrument};

    use fluvio_sc_schema::{AdminSpec, Status};
    use fluvio_auth::{AuthContext, InstanceAction};
    use fluvio_controlplane_metadata::{core::Spec, extended::SpecExt};

    use crate::services::auth::AuthServiceContext;

    /// Handler for object delete
    #[instrument(skip(auth_ctx, object_ctx, error_code, not_found_code))]
    pub async fn process<AC: AuthContext, S, F, G, C: MetadataItem>(
        name: String,
        auth_ctx: &AuthServiceContext<AC, C>,
        object_ctx: &StoreContext<S, C>,
        error_code: F,
        not_found_code: G,
    ) -> Result<Status, Error>
    where
        S: AdminSpec + SpecExt,
        <S as Spec>::IndexKey: TryFrom<String>,
        F: FnOnce(Error) -> ErrorCode,
        G: FnOnce() -> ErrorCode,
    {
        use fluvio_protocol::link::ErrorCode;

        info!(ty = %S::LABEL,%name, "deleting");

        if let Ok(authorized) = auth_ctx
            .auth
            .allow_instance_action(S::OBJECT_TYPE, InstanceAction::Delete, &name)
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

        let key = name
            .clone()
            .try_into()
            .map_err(|_err| Error::new(ErrorKind::InvalidData, "not convertible"))?;
        let status = if object_ctx.store().value(&key).await.is_some() {
            if let Err(err) = object_ctx.delete(key).await {
                let err_string = err.to_string();
                Status::new(name.clone(), error_code(err), Some(err_string))
            } else {
                info!(ty = %S::LABEL,%name, "deleted");
                Status::new_ok(name)
            }
        } else {
            Status::new(name, not_found_code(), Some("not found".to_owned()))
        };

        Ok(status)
    }
}

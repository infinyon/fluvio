//!
//! # Delete Topic Request
//!
//! Delete topic request handler. Lookup topic in local metadata, grab its K8 context
//! and send K8 a delete message.
//!
use dataplane::ErrorCode;
use tracing::{debug, instrument, trace};
use std::io::Error;

use dataplane::api::{RequestMessage, ResponseMessage};
use fluvio_sc_schema::{Status};
use fluvio_sc_schema::objects::{ObjectApiDeleteRequest};
use fluvio_auth::{AuthContext};

use crate::services::auth::AuthServiceContext;

/// Handler for delete topic request
#[instrument(skip(request, auth_ctx))]
pub async fn handle_delete_request<AC: AuthContext>(
    request: RequestMessage<ObjectApiDeleteRequest>,
    auth_ctx: &AuthServiceContext<AC>,
) -> Result<ResponseMessage<Status>, Error> {
    let (header, del_req) = request.get_header_request();

    debug!("del request: {:#?}", del_req);

    let status = match del_req {
        ObjectApiDeleteRequest::Topic(req) => {
            super::topic::handle_delete_topic(req.key(), auth_ctx).await?
        }
        ObjectApiDeleteRequest::CustomSpu(req) => {
            super::spu::handle_un_register_custom_spu_request(req.key(), auth_ctx).await?
        }
        ObjectApiDeleteRequest::SpuGroup(req) => {
            super::spg::handle_delete_spu_group(req.key(), auth_ctx).await?
        }
        ObjectApiDeleteRequest::ManagedConnector(req) => {
            super::connector::handle_delete_managed_connector(req.key(), auth_ctx).await?
        }
        ObjectApiDeleteRequest::SmartModule(req) => {
            super::smartmodule::handle_delete_smart_module(req.key(), auth_ctx).await?
        }
        ObjectApiDeleteRequest::Table(req) => {
            super::table::handle_delete_table(req.key(), auth_ctx).await?
        }
        ObjectApiDeleteRequest::SmartStream(req) => {
            delete_handler::process(
                req.key(),
                auth_ctx,
                auth_ctx.global_ctx.smartstreams(),
                |_| ErrorCode::SmartStreamObjectError,
                || ErrorCode::SmartModuleNotFound,
            )
            .await?
        }
    };

    trace!("flv delete topics resp {:#?}", status);

    Ok(ResponseMessage::from_header(&header, status))
}

mod delete_handler {
    use std::{
        convert::{TryFrom, TryInto},
        io::{Error, ErrorKind},
    };

    use dataplane::ErrorCode;
    use fluvio_stream_dispatcher::store::StoreContext;
    use tracing::{debug, trace, instrument};

    use fluvio_sc_schema::{AdminSpec, Status};
    use fluvio_auth::{AuthContext, InstanceAction};
    use fluvio_controlplane_metadata::{core::Spec, extended::SpecExt};

    use crate::services::auth::AuthServiceContext;

    /// Handler for object delete
    #[instrument(skip(auth_ctx, object_ctx, error_code, not_found_code))]
    pub async fn process<AC: AuthContext, S, F, G>(
        name: String,
        auth_ctx: &AuthServiceContext<AC>,
        object_ctx: &StoreContext<S>,
        error_code: F,
        not_found_code: G,
    ) -> Result<Status, Error>
    where
        S: AdminSpec + SpecExt,
        <S as Spec>::IndexKey: TryFrom<String>,
        F: FnOnce(Error) -> ErrorCode,
        G: FnOnce() -> ErrorCode,
    {
        use dataplane::ErrorCode;

        debug!(ty = %S::LABEL,%name,"deleting");

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
                Status::new_ok(name)
            }
        } else {
            Status::new(name, not_found_code(), Some("not found".to_owned()))
        };

        Ok(status)
    }
}

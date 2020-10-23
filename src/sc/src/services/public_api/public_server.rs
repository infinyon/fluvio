//!
//! # Public Sc Api Implementation
//!
//! Public service API allows 3rd party systems to invoke operations on Fluvio
//! Streaming Controller. Requests are received and dispatched to handlers
//! based on API keys.
//!

use std::sync::Arc;

use async_trait::async_trait;
use futures_util::io::AsyncRead;
use futures_util::io::AsyncWrite;
use event_listener::Event;

use fluvio_auth::identity::AuthorizationIdentity;
use fluvio_service::auth::Authorization;
use fluvio_service::api_loop;
use fluvio_service::call_service;
use fluvio_socket::InnerFlvSocket;
use fluvio_socket::FlvSocketError;
use fluvio_service::{FlvService};
use fluvio_sc_schema::AdminPublicApiKey;
use fluvio_sc_schema::AdminPublicRequest;
use fluvio_future::zero_copy::ZeroCopyWrite;

use crate::core::*;
use crate::services::auth::basic::{Policy, ScAuthorizationContext};

#[derive(Debug)]
pub struct PublicService {}

impl PublicService {
    pub fn new() -> Self {
        PublicService {}
    }
}

#[async_trait]
impl<S> FlvService<S, AuthorizationIdentity, Policy> for PublicService
where
    S: AsyncWrite + AsyncRead + Unpin + Send + ZeroCopyWrite + 'static,
{
    type Context = SharedContext;
    type IdentityContext = AuthorizationIdentity;
    type Request = AdminPublicRequest;
    type Authorization = ScAuthorizationContext;

    async fn respond(
        self: Arc<Self>,
        ctx: Self::Context,
        identity: Self::IdentityContext,
        socket: InnerFlvSocket<S>,
    ) -> Result<(), FlvSocketError> {
        let (sink, mut stream) = socket.split();
        let mut api_stream = stream.api_stream::<AdminPublicRequest, AdminPublicApiKey>();
        let mut shared_sink = sink.as_shared();

        let policy_config = ctx.config().policy.clone();

        let auth_context = AuthenticatedContext {
            global_ctx: ctx,
            auth: ScAuthorizationContext::create_authorization_context(identity, policy_config),
        };

        let end_event = Arc::new(Event::new());

        api_loop!(
            api_stream,
            "PublicAPI",

            AdminPublicRequest::ApiVersionsRequest(request) => call_service!(
                request,
                super::api_version::handle_api_versions_request(request),
                shared_sink,
                "api version handler"
            ),

            AdminPublicRequest::CreateRequest(request) => call_service!(
                request,
                super::create::handle_create_request(request, &auth_context),
                shared_sink,
                "create  handler"
            ),
            AdminPublicRequest::DeleteRequest(request) => call_service!(
                request,
                super::delete::handle_delete_request(request, &auth_context),
                shared_sink,
                "delete  handler"
            ),

            AdminPublicRequest::ListRequest(request) => call_service!(
                request,
                super::list::handle_list_request(request, &auth_context),
                shared_sink,
                "list handler"
            ),
            AdminPublicRequest::WatchRequest(request) =>

                super::watch::handle_watch_request(
                    request,
                    &auth_context,
                    shared_sink.clone(),
                    end_event.clone(),
                )

        );

        // we are done with this tcp stream, notify any controllers use this strep
        end_event.notify(usize::MAX);

        Ok(())
    }
}

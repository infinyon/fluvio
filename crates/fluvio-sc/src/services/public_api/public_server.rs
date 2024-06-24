//!
//! # Public Sc Api Implementation
//!
//! Public service API allows 3rd party systems to invoke operations on Fluvio
//! Streaming Controller. Requests are received and dispatched to handlers
//! based on API keys.
//!

use std::sync::Arc;
use std::marker::PhantomData;
use std::fmt::Debug;
use std::io::Error as IoError;

use tracing::debug;
use tracing::instrument;
use async_trait::async_trait;
use anyhow::Result;

use fluvio_service::ConnectInfo;
use fluvio_types::event::StickyEvent;
use fluvio_auth::Authorization;
use fluvio_stream_model::core::MetadataItem;
use fluvio_service::api_loop;
use fluvio_service::call_service;
use fluvio_socket::FluvioSocket;
use fluvio_service::FluvioService;
use fluvio_sc_schema::AdminPublicApiKey;
use fluvio_sc_schema::AdminPublicDecodedRequest;

use crate::services::auth::{AuthGlobalContext, AuthServiceContext};

#[derive(Debug)]
pub struct PublicService<A, C> {
    data: PhantomData<(A, C)>,
}

impl<A, C> PublicService<A, C> {
    pub fn new() -> Self {
        PublicService { data: PhantomData }
    }
}

#[async_trait]
impl<A, C> FluvioService for PublicService<A, C>
where
    A: Authorization + Sync + Send,
    C::UId: Send + Sync,
    C: MetadataItem + 'static,
    <A as Authorization>::Context: Send + Sync,
{
    type Context = AuthGlobalContext<A, C>;
    type Request = AdminPublicDecodedRequest;

    #[instrument(skip(self, ctx))]
    async fn respond(
        self: Arc<Self>,
        ctx: Self::Context,
        mut socket: FluvioSocket,
        _connection: ConnectInfo,
    ) -> Result<()> {
        let auth_context = ctx
            .auth
            .create_auth_context(&mut socket)
            .await
            .map_err(|err| {
                let io_error: IoError = err.into();
                io_error
            })?;

        debug!(?auth_context);
        let service_context = Arc::new(AuthServiceContext::new(
            ctx.global_ctx.clone(),
            auth_context,
        ));

        let (sink, mut stream) = socket.split();
        let mut api_stream = stream.api_stream::<AdminPublicDecodedRequest, AdminPublicApiKey>();
        let mut shared_sink = sink.as_shared();

        let end_event = StickyEvent::shared();

        api_loop!(
            api_stream,
            "PublicAPI",

            AdminPublicDecodedRequest::ApiVersionsRequest(request) => call_service!(
                request,
                super::api_version::handle_api_versions_request(request),
                shared_sink,
                "ApiVersionRequest"
            ),

            AdminPublicDecodedRequest::CreateRequest(request) => call_service!(
                request,
                super::create::handle_create_request(request, &service_context),
                shared_sink,
                "create  handler"
            ),
            AdminPublicDecodedRequest::UpdateRequest(request) => call_service!(
                request,
                super::update::handle_update_request(request, &service_context),
                shared_sink,
                "update handler"
            ),
            AdminPublicDecodedRequest::DeleteRequest(request) => call_service!(
                request,
                super::delete::handle_delete_request(request, &service_context),
                shared_sink,
                "delete  handler"
            ),

            AdminPublicDecodedRequest::ListRequest(request) => call_service!(
                request,
                super::list::handle_list_request(request, &service_context),
                shared_sink,
                "list handler"
            ),
            AdminPublicDecodedRequest::MirroringRequest(request) =>
                super::mirroring::handle_mirroring_request(request, service_context.clone(), shared_sink.clone(), end_event.clone())?,
            AdminPublicDecodedRequest::WatchRequest(request) =>
                super::watch::handle_watch_request(
                    request,
                    &service_context,
                    shared_sink.clone(),
                    end_event.clone(),
                )?

        );

        // we are done with this tcp stream, notify any controllers use this strep
        end_event.notify();

        Ok(())
    }
}

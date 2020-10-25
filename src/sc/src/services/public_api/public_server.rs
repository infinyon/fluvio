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

use async_trait::async_trait;
use futures_util::io::AsyncRead;
use futures_util::io::AsyncWrite;
use event_listener::Event;

use fluvio_auth::{ Authorization, AuthContext };
//use fluvio_service::aAuthorization;
use fluvio_service::api_loop;
use fluvio_service::call_service;
use fluvio_socket::InnerFlvSocket;
use fluvio_socket::FlvSocketError;
use fluvio_service::{FlvService};
use fluvio_sc_schema::AdminPublicApiKey;
use fluvio_sc_schema::AdminPublicRequest;
use fluvio_future::zero_copy::ZeroCopyWrite;

use crate::services::auth::{ AuthGlobalContext, AuthServiceContext };

#[derive(Debug)]
pub struct PublicService<A> {
    data: PhantomData<A>
}

impl <A> PublicService<A> {
    pub fn new() -> Self {
        PublicService {
            data: PhantomData
        }
    }
}

#[async_trait]
impl<S,A> FlvService<S> for PublicService<A>
where
    S: AsyncWrite + AsyncRead + Unpin + Send + ZeroCopyWrite + 'static,
    A: Authorization < Stream = S> + Sync + Send,
    <A as Authorization>::Context: Send + Sync ,
    AuthServiceContext<<A as Authorization>::Context>: AuthContext + Send + Sync,
  
{
    type Context = AuthGlobalContext<A>;
    type Request = AdminPublicRequest;

    async fn respond(
        self: Arc<Self>,
        ctx: Self::Context,
        mut socket: InnerFlvSocket<S>,
    ) -> Result<(), FlvSocketError> {

        let auth_context = ctx.auth.create_auth_context(&mut socket).await?;
        let service_context = Arc::new(AuthServiceContext::new(ctx.global_ctx.clone(),auth_context));

        let (sink, mut stream) = socket.split();
        let mut api_stream = stream.api_stream::<AdminPublicRequest, AdminPublicApiKey>();
        let mut shared_sink = sink.as_shared();

       
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
                super::create::handle_create_request(request, &service_context),
                shared_sink,
                "create  handler"
            ),
            AdminPublicRequest::DeleteRequest(request) => call_service!(
                request,
                super::delete::handle_delete_request(request, &service_context),
                shared_sink,
                "delete  handler"
            ),

            AdminPublicRequest::ListRequest(request) => call_service!(
                request,
                super::list::handle_list_request(request, &service_context),
                shared_sink,
                "list handler"
            ),
            AdminPublicRequest::WatchRequest(request) =>

                super::watch::handle_watch_request(
                    request,
                    &service_context,
                    shared_sink.clone(),
                    end_event.clone(),
                )

        );

        // we are done with this tcp stream, notify any controllers use this strep
        end_event.notify(usize::MAX);

        Ok(())
    }
}

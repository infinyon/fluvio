//!
//! # Service Implementataion
//!
//! Public service API allows 3rd party systems to invoke operations on Fluvio
//! Streaming Controller. Requests are received and dispatched to handlers
//! based on API keys.
//!

use std::sync::Arc;
use std::marker::PhantomData;

use async_trait::async_trait;
use futures::io::AsyncRead;
use futures::io::AsyncWrite;

use kf_service::api_loop;
use kf_service::call_service;
use kf_socket::InnerKfSocket;
use kf_socket::KfSocketError;
use kf_service::KfService;
use sc_api::ScPublicRequest;
use sc_api::ScPublicApiKey;
use k8_metadata_client::MetadataClient;
use flv_future_aio::zero_copy::ZeroCopyWrite;

use super::api::handle_api_versions_request;
use super::api::handle_kf_metadata_request;

use super::api::handle_create_topics_request;
use super::api::handle_delete_topics_request;
use super::api::handle_fetch_topics_request;
use super::api::handle_topic_composition_request;
use super::api::handle_register_custom_spus_request;
use super::api::handle_unregister_custom_spus_request;
use super::api::handle_fetch_spu_request;
use super::api::handle_create_spu_groups_request;
use super::api::handle_delete_spu_groups_request;
use super::api::handle_fetch_spu_groups_request;

use super::SharedPublicContext;

pub struct PublicService<C>(PhantomData<C>);

impl<C> PublicService<C> {
    pub fn new() -> Self {
        PublicService(PhantomData)
    }
}

#[async_trait]
impl<C, S> KfService<S> for PublicService<C>
where
    C: MetadataClient,
    S: AsyncWrite + AsyncRead + Unpin + Send + ZeroCopyWrite + 'static,
{
    type Context = SharedPublicContext<C>;
    type Request = ScPublicRequest;

    async fn respond(
        self: Arc<Self>,
        ctx: Self::Context,
        socket: InnerKfSocket<S>,
    ) -> Result<(), KfSocketError> {
        let (mut sink, mut stream) = socket.split();
        let mut api_stream = stream.api_stream::<ScPublicRequest, ScPublicApiKey>();

        api_loop!(
            api_stream,

            // Common
            ScPublicRequest::ApiVersionsRequest(request) => call_service!(
                request,
                handle_api_versions_request(request),
                sink,
                "api version handler"
            ),

            // Kafka
            ScPublicRequest::KfMetadataRequest(request) => call_service!(
                request,
                handle_kf_metadata_request(request, ctx.metadata.clone()),
                sink,
                "metadata request handler"
            ),

            // Fluvio - Topics
            ScPublicRequest::FlvCreateTopicsRequest(request) => call_service!(
                request,
                handle_create_topics_request(request, &ctx),
                sink,
                "create topic handler"
            ),
            ScPublicRequest::FlvDeleteTopicsRequest(request) => call_service!(
                request,
                handle_delete_topics_request(request, &ctx),
                sink,
                "delete topic handler"
            ),
            ScPublicRequest::FlvFetchTopicsRequest(request) => call_service!(
                request,
                handle_fetch_topics_request(request, ctx.metadata.clone()),
                sink,
                "fetch topic handler"
            ),
            ScPublicRequest::FlvTopicCompositionRequest(request) => call_service!(
                request,
                handle_topic_composition_request(request, ctx.metadata.clone()),
                sink,
                "topic metadata handler"
            ),

            // Fluvio - Spus
            ScPublicRequest::FlvRegisterCustomSpusRequest(request) => call_service!(
                request,
                handle_register_custom_spus_request(request, &ctx),
                sink,
                "create custom spus handler"
            ),
            ScPublicRequest::FlvUnregisterCustomSpusRequest(request) => call_service!(
                request,
                handle_unregister_custom_spus_request(request, &ctx),
                sink,
                "delete custom spus handler"
            ),
            ScPublicRequest::FlvFetchSpusRequest(request) => call_service!(
                request,
                handle_fetch_spu_request(request, ctx.metadata.clone()),
                sink,
                "fetch spus handler"
            ),

            ScPublicRequest::FlvCreateSpuGroupsRequest(request) => call_service!(
                request,
                handle_create_spu_groups_request(request, &ctx),
                sink,
                "create spu groups handler"
            ),
            ScPublicRequest::FlvDeleteSpuGroupsRequest(request) => call_service!(
                request,
                handle_delete_spu_groups_request(request, &ctx),
                sink,
                "delete spu groups handler"
            ),
            ScPublicRequest::FlvFetchSpuGroupsRequest(request) => call_service!(
                request,
                handle_fetch_spu_groups_request(request, &ctx),
                sink,
                "fetch spu groups handler"
            ),
            _ => {
                log::warn!("not actual protocol");
            }

        );

        Ok(())
    }
}

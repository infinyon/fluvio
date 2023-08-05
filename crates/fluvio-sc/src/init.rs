//!
//! # Iitialization routines for Streaming Coordinator (SC)
//!
//! All processing engines are hooked-up here. Channels are created and split between sencders
//! and receivers.
//!
use std::sync::Arc;

use fluvio_stream_model::core::MetadataItem;
#[cfg(feature = "k8")]
use k8_metadata_client::{MetadataClient, SharedClient};

use crate::core::Context;
use crate::core::SharedContext;
use crate::controllers::spus::SpuController;
use crate::controllers::topics::TopicController;
use crate::controllers::partitions::PartitionController;
#[cfg(feature = "k8")]
use crate::config::ScConfig;
use crate::services::start_internal_server;
#[cfg(feature = "k8")]
use crate::dispatcher::dispatcher::K8ClusterStateDispatcher;
use crate::services::auth::basic::BasicRbacPolicy;

#[cfg(feature = "k8")]
pub async fn start_main_loop_with_k8<C>(
    sc_config_policy: (ScConfig, Option<BasicRbacPolicy>),
    metadata_client: SharedClient<C>,
) -> crate::core::K8SharedContext
where
    C: MetadataClient + 'static,
{
    use crate::stores::spu::SpuSpec;
    use crate::stores::topic::TopicSpec;
    use crate::stores::partition::PartitionSpec;
    use crate::stores::spg::SpuGroupSpec;
    use crate::stores::tableformat::TableFormatSpec;
    use crate::stores::smartmodule::SmartModuleSpec;

    let (sc_config, auth_policy) = sc_config_policy;

    let namespace = sc_config.namespace.clone();
    let ctx = Context::shared_metadata(sc_config);

    K8ClusterStateDispatcher::<SpuSpec, C>::start(
        namespace.clone(),
        metadata_client.clone(),
        ctx.spus().clone(),
    );

    K8ClusterStateDispatcher::<TopicSpec, C>::start(
        namespace.clone(),
        metadata_client.clone(),
        ctx.topics().clone(),
    );

    K8ClusterStateDispatcher::<PartitionSpec, C>::start(
        namespace.clone(),
        metadata_client.clone(),
        ctx.partitions().clone(),
    );

    K8ClusterStateDispatcher::<SpuGroupSpec, C>::start(
        namespace.clone(),
        metadata_client.clone(),
        ctx.spgs().clone(),
    );

    K8ClusterStateDispatcher::<TableFormatSpec, C>::start(
        namespace.clone(),
        metadata_client.clone(),
        ctx.tableformats().clone(),
    );

    K8ClusterStateDispatcher::<SmartModuleSpec, C>::start(
        namespace,
        metadata_client,
        ctx.smartmodules().clone(),
    );

    start_main_loop(ctx, auth_policy).await
}

/// start the main loop
pub async fn start_main_loop<C>(
    ctx: Arc<Context<C>>,
    auth_policy: Option<BasicRbacPolicy>,
) -> SharedContext<C>
where
    C: MetadataItem + 'static,
    C::UId: Send + Sync,
{
    let config = ctx.config();
    whitelist!(config, "spu", SpuController::start(ctx.clone()));
    whitelist!(config, "topic", TopicController::start(ctx.clone()));
    whitelist!(
        config,
        "partition",
        PartitionController::start(ctx.partitions().clone(), ctx.spus().clone())
    );

    whitelist!(config, "internal", start_internal_server(ctx.clone()));
    whitelist!(
        config,
        "public",
        pub_server::start(ctx.clone(), auth_policy)
    );

    mod pub_server {

        use std::sync::Arc;
        use tracing::info;

        use crate::services::start_public_server;
        use crate::core::SharedContext;

        use fluvio_controlplane_metadata::core::MetadataItem;
        use crate::services::auth::{AuthGlobalContext, RootAuthorization};
        use crate::services::auth::basic::{BasicAuthorization, BasicRbacPolicy};

        pub fn start<C>(ctx: SharedContext<C>, auth_policy_option: Option<BasicRbacPolicy>)
        where
            C: MetadataItem + 'static,
            C::UId: Send + Sync,
        {
            if let Some(policy) = auth_policy_option {
                info!("using basic authorization");
                start_public_server(AuthGlobalContext::new(
                    ctx,
                    Arc::new(BasicAuthorization::new(policy)),
                ));
            } else {
                info!("using root authorization");
                start_public_server(AuthGlobalContext::new(
                    ctx,
                    Arc::new(RootAuthorization::new()),
                ));
            }
        }
    }

    ctx
}

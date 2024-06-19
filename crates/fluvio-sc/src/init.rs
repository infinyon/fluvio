//!
//! # Iitialization routines for Streaming Coordinator (SC)
//!
//! All processing engines are hooked-up here. Channels are created and split between sencders
//! and receivers.
//!
use std::sync::Arc;

use fluvio_sc_schema::mirror::MirrorSpec;
use fluvio_stream_dispatcher::metadata::{SharedClient, MetadataClient};
use fluvio_stream_model::core::MetadataItem;

use crate::controllers::mirroring::controller::RemoteMirrorController;
use crate::core::Context;
use crate::core::SharedContext;
use crate::controllers::partitions::PartitionController;
use crate::controllers::spus::SpuController;
use crate::controllers::topics::controller::{TopicController, SystemTopicController};
use crate::config::ScConfig;
use crate::services::start_internal_server;
use crate::dispatcher::dispatcher::MetadataDispatcher;
use crate::services::auth::basic::BasicRbacPolicy;

pub async fn start_main_loop<C, M>(
    sc_config_policy: (ScConfig, Option<BasicRbacPolicy>),
    metadata_client: SharedClient<C>,
) -> crate::core::SharedContext<M>
where
    C: MetadataClient<M> + 'static,
    M: MetadataItem,
    M::UId: Send + Sync,
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

    MetadataDispatcher::<SpuSpec, C, M>::start(
        namespace.clone(),
        metadata_client.clone(),
        ctx.spus().clone(),
    );

    MetadataDispatcher::<TopicSpec, C, M>::start(
        namespace.clone(),
        metadata_client.clone(),
        ctx.topics().clone(),
    );

    MetadataDispatcher::<PartitionSpec, C, M>::start(
        namespace.clone(),
        metadata_client.clone(),
        ctx.partitions().clone(),
    );

    MetadataDispatcher::<SpuGroupSpec, C, M>::start(
        namespace.clone(),
        metadata_client.clone(),
        ctx.spgs().clone(),
    );

    MetadataDispatcher::<TableFormatSpec, C, M>::start(
        namespace.clone(),
        metadata_client.clone(),
        ctx.tableformats().clone(),
    );

    MetadataDispatcher::<SmartModuleSpec, C, M>::start(
        namespace.clone(),
        metadata_client.clone(),
        ctx.smartmodules().clone(),
    );

    MetadataDispatcher::<MirrorSpec, C, M>::start(
        namespace.clone(),
        metadata_client.clone(),
        ctx.mirrors().clone(),
    );

    start_main_loop_services(ctx, auth_policy).await
}

/// start the main loop
async fn start_main_loop_services<C>(
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
    whitelist!(config, "topic", SystemTopicController::start(ctx.clone()));
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
    whitelist!(
        config,
        "mirroring",
        RemoteMirrorController::start(ctx.clone())
    );

    mod pub_server {

        use std::sync::Arc;
        use fluvio_auth::root::RootAuthorization;
        use tracing::info;

        use crate::services::start_public_server;
        use crate::core::SharedContext;

        use fluvio_controlplane_metadata::core::MetadataItem;
        use crate::services::auth::{AuthGlobalContext, ReadOnlyAuthorization};
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
            } else if ctx.config().read_only_metadata {
                info!("using read-only authorization");

                start_public_server(AuthGlobalContext::new(
                    ctx,
                    Arc::new(ReadOnlyAuthorization::new()),
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

//!
//! # Iitialization routines for Streaming Coordinator (SC)
//!
//! All processing engines are hooked-up here. Channels are created and split between sencders
//! and receivers.
//!

use k8_metadata_client::SharedClient;
use k8_metadata_client::MetadataClient;

use crate::core::Context;
use crate::core::SharedContext;
use crate::controllers::spus::SpuController;
use crate::controllers::topics::TopicController;
use crate::controllers::partitions::PartitionController;
use crate::config::ScConfig;
use crate::services::start_public_server;
use crate::services::start_internal_server;
use crate::dispatcher::dispatcher::K8ClusterStateDispatcher;

/// start the main loop
pub async fn start_main_loop<C>(
    sc_config: ScConfig,
    metadata_client: SharedClient<C>,
) -> SharedContext
where
    C: MetadataClient + 'static,
{
    use crate::stores::spu::SpuSpec;
    use crate::stores::topic::TopicSpec;
    use crate::stores::partition::PartitionSpec;
    use crate::stores::spg::SpuGroupSpec;

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

    SpuController::start(ctx.clone());
    TopicController::start(ctx.clone());
    PartitionController::start(ctx.clone());

    start_internal_server(ctx.clone());

    start_public_server(ctx.clone());

    ctx
}

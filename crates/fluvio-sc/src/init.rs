//!
//! # Iitialization routines for Streaming Coordinator (SC)
//!
//! All processing engines are hooked-up here. Channels are created and split between sencders
//! and receivers.
//!

use sysinfo::System;
use sysinfo::SystemExt;
use tracing::info;
use k8_metadata_client::SharedClient;
use k8_metadata_client::MetadataClient;

use crate::core::Context;
use crate::core::SharedContext;
use crate::controllers::spus::SpuController;
use crate::controllers::topics::TopicController;
use crate::controllers::partitions::PartitionController;
use crate::controllers::smartstreams::SmartStreamController;
use crate::config::{ScConfig};
use crate::services::start_internal_server;
use crate::dispatcher::dispatcher::K8ClusterStateDispatcher;
use crate::services::auth::basic::BasicRbacPolicy;

/// start the main loop
pub async fn start_main_loop<C>(
    sc_config_policy: (ScConfig, Option<BasicRbacPolicy>),
    metadata_client: SharedClient<C>,
) -> SharedContext
where
    C: MetadataClient + 'static,
{
    use crate::stores::spu::SpuSpec;
    use crate::stores::topic::TopicSpec;
    use crate::stores::partition::PartitionSpec;
    use crate::stores::spg::SpuGroupSpec;
    use crate::stores::connector::ManagedConnectorSpec;
    use crate::stores::table::TableSpec;
    use crate::stores::smartmodule::SmartModuleSpec;
    use crate::stores::smartstream::SmartStreamSpec;

    info!(PlatformVersion = &*crate::VERSION, "SC Platform Version");

    let mut sys = System::new_all();
    sys.refresh_all();
    info!(version = &*crate::VERSION, "Platform");
    info!(commit = env!("GIT_HASH"), "Git");
    info!(name = ?sys.name(),"System");
    info!(kernel = ?sys.kernel_version(),"System");
    info!(os_version = ?sys.long_os_version(),"System");
    info!(core_count = ?sys.physical_core_count(),"System");
    info!(total_memory = sys.total_memory(), "System");
    info!(available_memory = sys.available_memory(), "System");
    info!(uptime = sys.uptime(), "Uptime in secs");

    let (sc_config, auth_policy) = sc_config_policy;

    let namespace = sc_config.namespace.clone();
    let ctx = Context::shared_metadata(sc_config);
    let config = ctx.config();

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

    K8ClusterStateDispatcher::<ManagedConnectorSpec, C>::start(
        namespace.clone(),
        metadata_client.clone(),
        ctx.managed_connectors().clone(),
    );

    K8ClusterStateDispatcher::<TableSpec, C>::start(
        namespace.clone(),
        metadata_client.clone(),
        ctx.tables().clone(),
    );

    K8ClusterStateDispatcher::<SmartModuleSpec, C>::start(
        namespace.clone(),
        metadata_client.clone(),
        ctx.smart_modules().clone(),
    );

    K8ClusterStateDispatcher::<SmartStreamSpec, C>::start(
        namespace,
        metadata_client,
        ctx.smartstreams().clone(),
    );

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

    SmartStreamController::start(ctx.clone());

    mod pub_server {

        use std::sync::Arc;
        use tracing::info;

        use crate::services::start_public_server;
        use crate::core::SharedContext;

        use crate::services::auth::{AuthGlobalContext, RootAuthorization};
        use crate::services::auth::basic::{BasicAuthorization, BasicRbacPolicy};

        pub fn start(ctx: SharedContext, auth_policy_option: Option<BasicRbacPolicy>) {
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

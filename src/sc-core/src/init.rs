//!
//! # Iitialization routines for Streaming Coordinator (SC)
//!
//! All processing engines are hooked-up here. Channels are created and split between sencders
//! and receivers.
//!
use std::sync::Arc;

use k8_metadata_client::SharedClient;
use k8_metadata_client::MetadataClient;

use crate::conn_manager::ConnManager;

use crate::core::LocalStores;
use crate::core::ShareLocalStores;
use crate::core::WSUpdateService;
use crate::core::WSChangeDispatcher;
use crate::core::spus::SpuController;
use crate::core::topics::TopicController;
use crate::core::partitions::PartitionController;
use crate::config::ScConfig;
use crate::services::create_public_server;
use crate::services::create_internal_server;
use crate::services::InternalApiServer;
use crate::metadata::K8AllChangeDispatcher;
use crate::metadata::K8WSUpdateService;

/// start the main loop
pub async fn start_main_loop<C>(
    sc_config: ScConfig,
    metadata_client: SharedClient<C>,
) -> (K8WSUpdateService<C>, ShareLocalStores)
where
    C: MetadataClient + 'static,
    K8WSUpdateService<C>: Clone,
{
    let namespace = sc_config.namespace.clone();
    let local_stores = LocalStores::shared_metadata(sc_config);
    let k8_ws_service = K8WSUpdateService::new(metadata_client.clone());
    let mut k8_dispatcher = K8AllChangeDispatcher::new(
        metadata_client.clone(),
        namespace.clone(),
        local_stores.clone(),
    );
    let (metadata, internal_server) =
        create_core_services(local_stores, k8_ws_service.clone(), &mut k8_dispatcher);
    let public_server = create_public_server(metadata.clone(), k8_ws_service.clone(), namespace);

    let _public_shutdown = public_server.run();
    let _private_shutdown = internal_server.run();

    k8_dispatcher.run();

    (k8_ws_service, metadata)
}

/// essential core controllers
pub fn create_core_services<W, D>(
    local_stores: ShareLocalStores,
    ws_service: W,
    ws_dispatcher: &mut D,
) -> (ShareLocalStores, InternalApiServer)
where
    W: WSUpdateService + Clone + Sync + Send + 'static,
    D: WSChangeDispatcher,
{
    // connect conn manager and controllers
    let conn_manager = ConnManager::new_with_local_stores(local_stores.clone());
    let spu_lc_channel = ws_dispatcher.create_spu_channel();
    let topic_spu_channel = ws_dispatcher.create_spu_channel();
    let topic_topic_channel = ws_dispatcher.create_topic_channel();
    let partition_channel = ws_dispatcher.create_partition_channel();
    let partition_spu_channel = ws_dispatcher.create_spu_channel();

    let shared_conn_manager = Arc::new(conn_manager);

    // start controller
    let spu_controller = SpuController::new(
        local_stores.clone(),
        shared_conn_manager.clone(),
        spu_lc_channel,
        ws_service.clone(),
    );

    let partition_controller = PartitionController::new(
        local_stores.clone(),
        shared_conn_manager.clone(),
        partition_channel,
        partition_spu_channel,
        ws_service.clone(),
    );

    let private_server = create_internal_server(
        local_stores.clone(),
        shared_conn_manager,
        spu_controller.conn_sender(),
        partition_controller.lrs_sender(),
    );

    spu_controller.run();

    let topic_controller = TopicController::new(
        local_stores.clone(),
        topic_spu_channel,
        topic_topic_channel,
        ws_service.clone(),
    );

    topic_controller.run();
    partition_controller.run();
    (local_stores, private_server)
}

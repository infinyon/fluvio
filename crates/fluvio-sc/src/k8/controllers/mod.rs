pub mod spg_stateful;
pub mod spu_service;
pub mod spu_controller;

pub use k8_operator::run_k8_operators;

mod k8_operator {

    use fluvio_stream_dispatcher::metadata::{SharedClient, MetadataClient};
    use fluvio_stream_model::store::k8::K8MetaItem;
    use tracing::info;

    use crate::cli::TlsConfig;
    use crate::core::K8SharedContext;
    use crate::stores::StoreContext;
    use crate::dispatcher::dispatcher::ClusterStateDispatcher;
    use crate::k8::objects::spu_service::SpuServiceSpec;
    use crate::k8::objects::statefulset::StatefulsetSpec;
    use crate::k8::objects::spg_service::SpgServiceSpec;
    use crate::k8::objects::spu_k8_config::ScK8Config;

    use crate::k8::controllers::spg_stateful::SpgStatefulSetController;
    use crate::k8::controllers::spu_service::SpuServiceController;
    use crate::k8::controllers::spu_controller::K8SpuController;

    pub async fn run_k8_operators<C: MetadataClient<K8MetaItem> + 'static>(
        namespace: String,
        client: SharedClient<C>,
        global_ctx: K8SharedContext,
        tls: Option<TlsConfig>,
    ) {
        let config = global_ctx.config();

        let spu_service_ctx: StoreContext<SpuServiceSpec> = StoreContext::new();
        let statefulset_ctx: StoreContext<StatefulsetSpec> = StoreContext::new();
        let spg_service_ctx: StoreContext<SpgServiceSpec> = StoreContext::new();

        let config_ctx: StoreContext<ScK8Config> = StoreContext::new();

        info!("starting k8 cluster operators");

        ClusterStateDispatcher::<_, _, K8MetaItem>::start(
            namespace.clone(),
            client.clone(),
            spu_service_ctx.clone(),
        );

        ClusterStateDispatcher::<_, _, K8MetaItem>::start(
            namespace.clone(),
            client.clone(),
            statefulset_ctx.clone(),
        );

        ClusterStateDispatcher::<_, _, K8MetaItem>::start(
            namespace.clone(),
            client.clone(),
            spg_service_ctx.clone(),
        );

        ClusterStateDispatcher::<_, _, K8MetaItem>::start(
            namespace.clone(),
            client,
            config_ctx.clone(),
        );

        whitelist!(config, "k8_spg", {
            SpgStatefulSetController::start(
                namespace,
                config_ctx.clone(),
                global_ctx.spgs().clone(),
                statefulset_ctx,
                global_ctx.spus().clone(),
                spg_service_ctx,
                tls,
            );
        });

        whitelist!(config, "k8_spu", {
            K8SpuController::start(
                global_ctx.spus().clone(),
                spu_service_ctx.clone(),
                global_ctx.spgs().clone(),
            );
        });

        whitelist!(config, "k8_spu_service", {
            SpuServiceController::start(config_ctx, spu_service_ctx, global_ctx.spgs().clone());
        });
    }
}

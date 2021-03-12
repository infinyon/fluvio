pub mod spg;
pub mod spu_service;
pub mod spu;

pub use k8_operator::run_k8_operators;

mod k8_operator {
    use tracing::error;

    use k8_client::SharedK8Client;

    use crate::cli::TlsConfig;
    use crate::core::SharedContext;
    use crate::stores::StoreContext;
    use crate::dispatcher::dispatcher::K8ClusterStateDispatcher;
    use crate::k8::objects::spu_service::SpuServicespec;
    use crate::k8::objects::spu_k8_config::ScK8Config;
    use crate::k8::objects::statefulset::StatefulsetSpec;
    use crate::k8::objects::spg_service::SpgServiceSpec;
    use crate::k8::controllers::spg::SpgStatefulSetController;
    use crate::k8::controllers::spu_service::SpuServiceController;
    use crate::k8::controllers::spu::SpuController;

    pub async fn run_k8_operators(
        namespace: String,
        k8_client: SharedK8Client,
        global_ctx: SharedContext,
        tls: Option<TlsConfig>,
    ) {
        let spu_service_ctx: StoreContext<SpuServicespec> = StoreContext::new();
        let statefulset_ctx: StoreContext<StatefulsetSpec> = StoreContext::new();
        let spg_service_ctx: StoreContext<SpgServiceSpec> = StoreContext::new();

        K8ClusterStateDispatcher::<_, _>::start(
            namespace.clone(),
            k8_client.clone(),
            spu_service_ctx.clone(),
        );

        K8ClusterStateDispatcher::<_, _>::start(
            namespace.clone(),
            k8_client.clone(),
            statefulset_ctx.clone(),
        );

        K8ClusterStateDispatcher::<_, _>::start(
            namespace.clone(),
            k8_client.clone(),
            spg_service_ctx.clone(),
        );

        SpgStatefulSetController::start(
            k8_client.clone(),
            namespace.clone(),
            global_ctx.spgs().clone(),
            statefulset_ctx,
            global_ctx.spus().clone(),
            spg_service_ctx,
            tls,
        );

        let disable_spu = match ScK8Config::load(&k8_client, &namespace).await {
            Ok(config) => config.sc_config.disable_spu,
            Err(err) => {
                error!("error loading config: {:#?}", err);
                false
            }
        };

        SpuController::start(
            global_ctx.spus().clone(),
            spu_service_ctx.clone(),
            global_ctx.spgs().clone(),
            disable_spu,
        );

        SpuServiceController::start(
            k8_client.clone(),
            namespace.clone(),
            spu_service_ctx.clone(),
            global_ctx.spgs().clone(),
        );
    }
}

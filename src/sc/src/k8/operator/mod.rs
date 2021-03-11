mod spg_operator;
mod spg_group;
mod spg_service;
mod spu_k8_config;
mod statefulset;

pub use spu_k8_config::ScK8Config;
pub use k8_operator::run_k8_operators;

mod k8_operator {
    use k8_client::SharedK8Client;

  
    use crate::cli::TlsConfig;
    use crate::core::SharedContext;
    use crate::stores::StoreContext;
    use crate::dispatcher::dispatcher::K8ClusterStateDispatcher;
    use crate::k8::service::SpuServicespec;
    use crate::k8::service::SpuServiceController;

    use super::{ScK8Config};
    use super::spg_operator::SpgStatefulSetController;
    use super::statefulset::StatefulsetSpec;
    use super::spg_service::SpgServiceSpec;

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
             spu_service_ctx.clone(),
             tls
            );

    
        let config =  ScK8Config::load(&k8_client, &namespace).await.expect("error loading config");

        if !config.sc_config.disable_spu {
            SpuServiceController::start(global_ctx, spu_service_ctx);
        }
    }
}

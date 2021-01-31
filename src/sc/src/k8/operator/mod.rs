mod spg_operator;
mod conversion;
mod spg_group;
mod spu_k8_config;



pub use spu_k8_config::ScK8Config;
pub use operator::run_k8_operators;

mod operator {
    use k8_client::SharedK8Client;

    use super::{ScK8Config};
    use super:: spg_operator::SpgOperator;


    use crate::cli::TlsConfig;
    use crate::core::SharedContext;
    use crate::stores::StoreContext;
    use crate::dispatcher::dispatcher::K8ClusterStateDispatcher;
    use crate::k8::service::SpuServicespec;
    use crate::k8::service::SpuServiceController;

    pub fn run_k8_operators(
        namespace: String,
        k8_client: SharedK8Client,
        ctx: SharedContext,
        tls: Option<TlsConfig>,
        sc_config: &ScK8Config
    ) {
        SpgOperator::new(k8_client.clone(), namespace.clone(), ctx.clone(), tls).run();

        let svc_ctx: StoreContext<SpuServicespec> = StoreContext::new();

        K8ClusterStateDispatcher::<SpuServicespec, _>::start(namespace, k8_client, svc_ctx.clone());

        if !sc_config.sc_config.disable_spu {

            SpuServiceController::start(ctx, svc_ctx);
        }
    }
}

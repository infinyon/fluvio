mod spg_operator;
mod conversion;
mod spg_group;
mod spu_k8_config;

pub use spu_k8_config::ScK8Config;
pub use k8_operator::run_k8_operators;

mod k8_operator {
    use k8_client::SharedK8Client;

    use tracing::error;

    use crate::cli::TlsConfig;
    use crate::core::SharedContext;
    use crate::stores::StoreContext;
    use crate::dispatcher::dispatcher::K8ClusterStateDispatcher;
    use crate::k8::service::SpuServicespec;
    use crate::k8::service::SpuServiceController;

    use super::{ScK8Config};
    use super::spg_operator::SpgOperator;

    pub async fn run_k8_operators(
        namespace: String,
        k8_client: SharedK8Client,
        ctx: SharedContext,
        tls: Option<TlsConfig>,
    ) {
        SpgOperator::new(k8_client.clone(), namespace.clone(), ctx.clone(), tls).run();

        let svc_ctx: StoreContext<SpuServicespec> = StoreContext::new();

        K8ClusterStateDispatcher::<SpuServicespec, _>::start(
            namespace.clone(),
            k8_client.clone(),
            svc_ctx.clone(),
        );

        let disable_spu = match ScK8Config::load(&k8_client, &namespace).await {
            Ok(config) => config.sc_config.disable_spu,
            Err(err) => {
                error!("error loading config: {:#?}", err);
                false
            }
        };

        if !disable_spu {
            SpuServiceController::start(ctx, svc_ctx);
        }
    }
}

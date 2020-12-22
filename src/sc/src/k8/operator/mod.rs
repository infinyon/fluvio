mod spg_operator;
mod conversion;
mod spg_group;
mod spu_k8_config;

use k8_client::SharedK8Client;

use conversion::convert_cluster_to_statefulset;
use conversion::generate_service;
use spg_group::SpuGroupObj;
use spg_group::SpuValidation;
use spg_operator::SpgOperator;
use spu_k8_config::SpuK8Config;

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
) {
    SpgOperator::new(k8_client.clone(), namespace.clone(), ctx.clone(), tls).run();

    let svc_ctx: StoreContext<SpuServicespec> = StoreContext::new();

    K8ClusterStateDispatcher::<SpuServicespec, _>::start(namespace, k8_client, svc_ctx.clone());
    SpuServiceController::start(ctx, svc_ctx);
}

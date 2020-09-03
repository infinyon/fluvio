mod spg_operator;
mod conversion;
mod spg_group;

use flv_sc_core::core::SharedContext;
use k8_client::SharedK8Client;

use spg_operator::SpgOperator;

use self::conversion::convert_cluster_to_statefulset;
use self::conversion::generate_service;
use self::spg_group::SpuGroupObj;
use self::spg_group::SpuValidation;
use crate::service::SpuServicespec;
use crate::service::SpuServiceController;
use crate::stores::StoreContext;
use crate::cli::TlsConfig;
use crate::dispatcher::dispatcher::K8ClusterStateDispatcher;

pub fn run_k8_operators(
    namespace: String,
    k8_client: SharedK8Client,
    ctx: SharedContext,
    tls: Option<TlsConfig>,
) {
    SpgOperator::new(k8_client.clone(), namespace.clone(), ctx.clone(), tls).run();

    let svc_ctx: StoreContext<SpuServicespec> = StoreContext::new();

    K8ClusterStateDispatcher::<SpuServicespec, _>::start(namespace, k8_client,svc_ctx.clone());
    SpuServiceController::start(ctx,svc_ctx);

}

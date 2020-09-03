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

use crate::cli::TlsConfig;

pub fn run_k8_operators(
    namespace: String,
    k8_client: SharedK8Client,
    ctx: SharedContext,
    tls: Option<TlsConfig>,
) {
    SpgOperator::new(k8_client.clone(), namespace.clone(), ctx.clone(), tls).run();

   // SvcOperator::run(k8_client, namespace, ctx);
}

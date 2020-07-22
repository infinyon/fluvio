mod spg_operator;
mod conversion;
mod spg_group;
mod svc_operator;

use flv_sc_core::stores::spu::SharedSpuLocalStore;
use flv_sc_core::stores::K8MetaItem;
use k8_client::SharedK8Client;

use spg_operator::SpgOperator;
use svc_operator::SvcOperator;

use self::conversion::convert_cluster_to_statefulset;
use self::conversion::generate_service;
use self::spg_group::SpuGroupObj;
use self::spg_group::SpuValidation;

use crate::cli::TlsConfig;

pub fn run_k8_operators(
    namespace: String,
    k8_client: SharedK8Client,
    spu_store: SharedSpuLocalStore<K8MetaItem>,
    tls: Option<TlsConfig>,
) {
    SpgOperator::new(k8_client.clone(), namespace.clone(), spu_store.clone(), tls).run();

    SvcOperator::run(k8_client.clone(), namespace, spu_store);
}

mod spg_operator;
mod conversion;
mod spg_group;
mod svc_operator;

use flv_sc_core::metadata::K8WSUpdateService;
use flv_sc_core::core::spus::SharedSpuLocalStore;
use k8_client::K8Client;

use spg_operator::SpgOperator;
use svc_operator::SvcOperator;

use self::conversion::convert_cluster_to_statefulset;
use self::conversion::generate_service;
use self::spg_group::SpuGroupObj;
use self::spg_group::SpuValidation;

pub fn run_k8_operators(
    k8_ws: K8WSUpdateService<K8Client>,
    namespace: String,
    spu_store: SharedSpuLocalStore,
) {
    SpgOperator::new(k8_ws.own_client(), namespace.clone(), spu_store.clone()).run();
    SvcOperator::new(k8_ws, namespace, spu_store).run();
}

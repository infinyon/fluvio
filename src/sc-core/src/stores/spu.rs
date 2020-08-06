pub use flv_metadata_cluster::spu::store::*;
pub use flv_metadata_cluster::spu::*;
pub use flv_metadata_cluster::store::k8::K8MetaItem;

pub type SpuAdminMd = SpuMetadata<K8MetaItem>;
pub type SpuAdminStore = SpuLocalStore<K8MetaItem>;

// check if given range is conflict with any of the range
pub async fn is_conflict(
    spu_store: &SpuAdminStore,
    owner_uid: &str,
    start: i32,
    end_exclusive: i32,
) -> Option<i32> {
    for (_, spu) in spu_store.read().await.iter() {
        if !spu.is_owned(&owner_uid.to_string()) {
            let id = spu.spec.id;
            if id >= start && id < end_exclusive {
                return Some(id);
            }
        }
    }
    None
}

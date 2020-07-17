

pub use flv_metadata::spu::store::*;
pub use flv_metadata::spu::*;


use super::*;

pub type SpuAdminMd = SpuMetadata<K8MetaItem>;
pub type SpuAdminStore = SpuLocalStore<K8MetaItem>;



impl K8ExtendedSpec for SpuSpec {
    type K8Spec  = Self;
    type K8Status = Self::Status;
}


// check if given range is conflict with any of the range
pub async fn is_conflict(
    spu_store: &SpuAdminStore,
    owner_uid: &String,
    start: i32,
    end_exclusive: i32,
) -> Option<i32>
 {
    for (_, spu) in spu_store.read().await.iter() {
        if !spu.is_owned(owner_uid) {
            let id = spu.id();
            if id >= start && id < end_exclusive {
                return Some(id);
            }
        }
    }
    None
}
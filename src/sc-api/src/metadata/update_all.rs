use std::collections::BTreeMap;


use kf_protocol::derive::Decode;
use kf_protocol::derive::Encode;
use flv_types::SpuId;

use crate::objects::Metadata;
use crate::spu::SpuSpec;
use crate::partition::PartitionSpec;


/// All specs.  Listener can use this to sync their own metadata store.
#[derive(Decode, Encode, Debug, Default)]
pub struct UpdateAllMetadataResponse {
    pub spus: Vec<Metadata<SpuSpec>>,
    pub spu_epoch: i64,
    pub partitions: Vec<Metadata<PartitionSpec>>,
    pub partition_epoch: i64
}

impl std::fmt::Display for UpdateAllMetadataResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "spus: {}:{}, partitions: {}:{}", self.spu_epoch,self.spus.len(),self.partition_epoch,self.partitions.len())
    }
}

impl UpdateAllMetadataResponse {
    pub fn new(
        spus: Vec<Metadata<SpuSpec>>, 
        spu_epoch: i64,
        partitions: Vec<Metadata<PartitionSpec>>,
        partition_epoch: i64
    ) -> Self {
        Self { 
            spus,
            spu_epoch,
            partitions,
            partition_epoch
         }
    }


    pub fn spu_epoch(&self) -> i64 {
        self.spu_epoch
    }

    pub fn partition_epoch(&self) ->i64 {
        self.partition_epoch
    }

    
    pub fn spus(&self) -> &Vec<Metadata<SpuSpec>> {
        &self.spus
    }

    pub fn spus_owned(self) -> Vec<Metadata<SpuSpec>> {
        self.spus
    }

    pub fn spus_to_map(&self) -> BTreeMap<SpuId, SpuSpec> {
        let mut res = BTreeMap::new();
        for spu in self.spus.iter() {
            res.insert(spu.spec.id.clone(), spu.spec.clone());
        }
        res
    }

    
}


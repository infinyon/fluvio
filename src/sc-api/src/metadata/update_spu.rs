use std::collections::BTreeMap;
use kf_protocol::derive::Decode;
use kf_protocol::derive::Encode;
use flv_types::SpuId;
use flv_metadata::spu::SpuSpec;
use flv_metadata::message::*;

use crate::objects::Metadata;


pub type SpuUpdate = Message<Metadata<SpuSpec>>;

/// Changes to Spu specs
#[derive(Decode, Encode, Debug, Default, Clone)]
pub struct UpdateSpuResponse {
    epoch: i64,
    pub spus: Vec<SpuUpdate>
}
impl std::fmt::Display for UpdateSpuResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "spus: {}:{}", self.epoch,self.spus.len())
    }
}


impl UpdateSpuResponse {
    pub fn new(epoch: i64,spus: Vec<SpuUpdate>) -> Self {
        Self { 
            spus,
            epoch
         }
    }

    pub fn spus(&self) -> &Vec<SpuUpdate> {
        &self.spus
    }

    pub fn epoch(&self) -> i64 {
        self.epoch
    }

    pub fn spus_owned(self) -> Vec<SpuUpdate> {
        self.spus
    }

}
use std::collections::BTreeMap;

use kf_protocol::api::Request;
use kf_protocol::derive::Decode;
use kf_protocol::derive::Encode;
use flv_types::SpuId;
use flv_metadata::spu::SpuSpec;
use flv_metadata::api::SpuMsg;

use crate::InternalSpuApi;

/// Changes to Spu specs
#[derive(Decode, Encode, Debug, Default)]
pub struct UpdateSpuRequest {
    pub spus: Vec<SpuMsg>,
}

impl Request for UpdateSpuRequest {
    const API_KEY: u16 = InternalSpuApi::UpdateSpu as u16;
    type Response = UpdateSpuResponse;
}

impl UpdateSpuRequest {
    pub fn new(spus: Vec<SpuMsg>) -> Self {
        Self { spus }
    }

    pub fn spus_ref(&self) -> &Vec<SpuMsg> {
        &self.spus
    }

    pub fn spus(self) -> Vec<SpuMsg> {
        self.spus
    }

    pub fn spus_to_map(&self) -> BTreeMap<SpuId, SpuSpec> {
        let mut res = BTreeMap::new();
        for spu in self.spus.iter() {
            res.insert(spu.content.id.clone(), spu.content.clone());
        }
        res
    }

    pub fn add<S>(mut self, spu: S) -> Self
    where
        S: Into<SpuMsg>,
    {
        self.spus.push(spu.into());
        self
    }
}

#[derive(Decode, Encode, Default, Debug)]
pub struct UpdateSpuResponse {}

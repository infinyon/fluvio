#![allow(clippy::assign_op_pattern)]

use fluvio_protocol::api::Request;
use fluvio_protocol::{Encoder, Decoder};
use fluvio_controlplane_metadata::spu::SpuSpec;
use fluvio_controlplane_metadata::message::SpuMsg;

use crate::InternalSpuApi;

/// Changes to Spu specs
#[derive(Decoder, Encoder, Debug, Default)]
pub struct UpdateSpuRequest {
    pub epoch: i64,
    pub changes: Vec<SpuMsg>,
    pub all: Vec<SpuSpec>,
}

impl Request for UpdateSpuRequest {
    const API_KEY: u16 = InternalSpuApi::UpdateSpu as u16;
    type Response = UpdateSpuResponse;
}

impl UpdateSpuRequest {
    pub fn with_changes(epoch: i64, changes: Vec<SpuMsg>) -> Self {
        Self {
            epoch,
            changes,
            all: vec![],
        }
    }

    pub fn with_all(epoch: i64, all: Vec<SpuSpec>) -> Self {
        Self {
            epoch,
            changes: vec![],
            all,
        }
    }

    pub fn changes(&self) -> &Vec<SpuMsg> {
        &self.changes
    }

    pub fn changes_owned(self) -> Vec<SpuMsg> {
        self.changes
    }

    /*
    pub fn spus_to_map(&self) -> BTreeMap<SpuId, SpuSpec> {
        let mut res = BTreeMap::new();
        for spu in self.spus.iter() {
            res.insert(spu.content.id.clone(), spu.content.clone());
        }
        res
    }
    */

    /*
    pub fn add<S>(mut self, spu: S) -> Self
    where
        S: Into<SpuMsg>,
    {
        self.spus.push(spu.into());
        self
    }
    */
}

#[derive(Decoder, Encoder, Default, Debug)]
pub struct UpdateSpuResponse {}

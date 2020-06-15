use std::collections::BTreeMap;

use kf_protocol::api::Request;
use kf_protocol::derive::Decode;
use kf_protocol::derive::Encode;
use flv_metadata::partition::ReplicaKey;
use flv_metadata::spu::SpuSpec;
use flv_types::SpuId;

use super::ScServerApiKey;
use super::replica::ReplicaLeader;

#[derive(Decode, Encode, Default, Debug)]
pub struct UpdateAllMetadataRequest {}

impl Request for UpdateAllMetadataRequest {
    const API_KEY: u16 = ScServerApiKey::FlvUpdateAllMetadata as u16;
    type Response = UpdateAllMetadataResponse;
}

/// All metadata coming from SC to Client
#[derive(Decode, Encode, Debug, Default)]
pub struct UpdateAllMetadataResponse {
    pub spus: Vec<SpuSpec>,
    pub replicas: Vec<ReplicaLeader>,
}

impl UpdateAllMetadataResponse {
    pub fn new(spus: Vec<SpuSpec>, replicas: Vec<ReplicaLeader>) -> Self {
        Self { spus, replicas }
    }

    /// Used when only SPU spec changes
    pub fn new_with_spu(spus: Vec<SpuSpec>) -> Self {
        Self::new(spus, vec![])
    }

    pub fn spus_ref(&self) -> &Vec<SpuSpec> {
        &self.spus
    }

    pub fn spus(self) -> Vec<SpuSpec> {
        self.spus
    }

    pub fn spus_to_map(&self) -> BTreeMap<SpuId, SpuSpec> {
        let mut res = BTreeMap::new();
        for spu in self.spus.iter() {
            res.insert(spu.id.clone(), spu.clone());
        }
        res
    }

    pub fn replicas_to_map(&self) -> BTreeMap<ReplicaKey, ReplicaLeader> {
        let mut res: BTreeMap<ReplicaKey, ReplicaLeader> = BTreeMap::new();
        for replica in self.replicas.iter() {
            res.insert(replica.id.clone(), replica.clone());
        }
        res
    }

    pub fn push_spu(&mut self, msg: SpuSpec) {
        self.spus.push(msg);
    }

    pub fn add_spu<S>(mut self, spu: S) -> Self
    where
        S: Into<SpuSpec>,
    {
        self.spus.push(spu.into());
        self
    }

    pub fn mut_add_spu<S>(&mut self, spu: S)
    where
        S: Into<SpuSpec>,
    {
        self.spus.push(spu.into());
    }

    pub fn add_replica<R>(mut self, replica: R) -> Self
    where
        R: Into<ReplicaLeader>,
    {
        self.replicas.push(replica.into());
        self
    }

    pub fn add_replica_by_ref<R>(&mut self, replica: R)
    where
        R: Into<ReplicaLeader>,
    {
        self.replicas.push(replica.into());
    }
}

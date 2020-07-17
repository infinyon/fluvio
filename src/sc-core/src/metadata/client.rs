use sc_api::metadata::*;
use flv_metadata::store::actions::*;

use sc_api::topic::TopicSpec;

use crate::stores::K8MetaItem;

/// notify client of changes
#[derive(Debug,Clone)]
pub enum ClientNotification {
    SPU(UpdateSpuResponse),
    Replica(UpdateReplicaResponse),
    Topic
}

/*
impl From<Vec<LSChange<SpuSpec,K8MetaItem>>> for ClientNotification {

    fn from(actions: Vec<LSChange<SpuSpec,K8MetaItem>>) -> Self {

        Self::SPU(UpdateSpuResponse::new(
            actions.into_iter()
                    .map(|action| action.into())
                    .collect()
        ))

    }
}

impl From<Vec<LSChange<PartitionSpec,K8MetaItem>>> for ClientNotification {

    fn from(actions: Vec<LSChange<PartitionSpec,K8MetaItem>>) -> Self {

        Self::Replica(UpdateReplicaResponse::new(
            actions.into_iter()
                    .map(|action| action.into())
                    .collect()
        ))

    }
}
*/


impl From<Vec<LSChange<TopicSpec,K8MetaItem>>> for ClientNotification {

    fn from(actions: Vec<LSChange<TopicSpec,K8MetaItem>>) -> Self {

        Self::Topic

    }
}





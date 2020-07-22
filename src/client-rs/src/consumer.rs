use std::io::Error as IoError;
use std::io::ErrorKind;

use log::debug;
use log::trace;

use crate::kf::api::ReplicaKey;
use crate::kf::api::RecordSet;
use crate::kf::api::PartitionOffset;
use crate::kf::message::fetch::FetchablePartitionResponse;
use flv_api_spu::server::fetch_offset::{FlvFetchOffsetsRequest};
use flv_api_spu::server::fetch_offset::FetchOffsetPartitionResponse;

use crate::ClientError;
use crate::params::FetchOffset;
use crate::params::FetchLogOption;
use crate::client::RawClient;
use crate::client::Client;
use crate::spu::SpuPool;

/// consume message from replica leader
pub struct Consumer {
    replica: ReplicaKey,
    pool: SpuPool
}

impl Consumer  {

    pub fn new(replica: ReplicaKey, pool: SpuPool) -> Self {
        Self {
            replica,
            pool
        }
    }

    pub fn replica(&self) -> &ReplicaKey {
        &self.replica
    }


    pub async fn fetch_logs_once(
        &mut self,
        offset_option: FetchOffset,
        option: FetchLogOption,
    ) -> Result<FetchablePartitionResponse<RecordSet>, ClientError> {

        use kf_protocol::message::fetch::DefaultKfFetchRequest;
        use kf_protocol::message::fetch::FetchPartition;
        use kf_protocol::message::fetch::FetchableTopic;

        debug!(
            "starting fetch log once: {:#?} from replica: {}",
            offset_option,
            self.replica,
        );

        let mut leader = self.pool.spu_leader(&self.replica).await?;

        debug!(
            "found spu leader {}",
            leader
        );


        let offset = calc_offset(&mut leader,&self.replica,offset_option).await?;

        let partition = FetchPartition {
            partition_index: self.replica.partition,
            fetch_offset: offset,
            max_bytes: option.max_bytes,
            ..Default::default()
        };

        let topic_request = FetchableTopic {
            name: self.replica.topic.to_owned(),
            fetch_partitions: vec![partition],
            ..Default::default()
        };

        let fetch_request = DefaultKfFetchRequest {
            topics: vec![topic_request],
            isolation_level: option.isolation,
            max_bytes: option.max_bytes,
            ..Default::default()
        };

        let response = leader.send_receive(fetch_request).await?;

        debug!(
            "received fetch logs for {}",
            self.replica
        );

        if let Some(partition_response) = response.find_partition(&self.replica.topic, self.replica.partition) {
            debug!(
                "found partition response with: {} batches: {} bytes",
                partition_response.records.batches.len(),
                bytes_count(&partition_response.records)
            );
            Ok(partition_response)
        } else {
            Err(ClientError::PartitionNotFound(
                self.replica.to_owned()
            ))
        }
    }
}


async fn fetch_offsets(client: &mut RawClient,replica: &ReplicaKey) -> Result<FetchOffsetPartitionResponse, ClientError> {
    debug!("fetching offset for replica: {}", replica);

    let response = client
        .send_receive(FlvFetchOffsetsRequest::new(
            replica.topic.to_owned(),
            replica.partition,
        ))
        .await?;

    trace!(
        "receive fetch response replica: {}, {:#?}",
        replica,
        response
    );

    match response.find_partition(&replica) {
        Some(partition_response) => {
            debug!("replica: {}, fetch offset: {}",replica,partition_response);
            Ok(partition_response)
        }
        None => Err(IoError::new(
            ErrorKind::InvalidData,
            format!(
                "no replica offset for: {}",
                replica
            ),
        )
        .into()),
    }
}

/// depends on offset option, calculate offset

async fn calc_offset(client: &mut RawClient, replica: &ReplicaKey,offset: FetchOffset) -> Result<i64, ClientError> {
    Ok(match offset {
        FetchOffset::Offset(inner_offset) => inner_offset,
        FetchOffset::Earliest(relative_offset) => {
            let offsets = fetch_offsets(client,replica).await?;
            offsets.start_offset() + relative_offset.unwrap_or(0)
        }
        FetchOffset::Latest(relative_offset) => {
            let offsets = fetch_offsets(client,replica).await?;
            offsets.last_stable_offset() - relative_offset.unwrap_or(0)
        }
    })
}

/// compute total bytes in record set
fn bytes_count(records: &RecordSet) -> usize {

    records.batches.iter()
        .map(|batch| batch.records.iter().map(|record| record.value.len()).sum::<usize>()).sum()
}
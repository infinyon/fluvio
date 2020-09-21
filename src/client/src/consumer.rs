use std::io::Error as IoError;
use std::io::ErrorKind;

use tracing::debug;
use tracing::trace;

use kf_socket::AsyncResponse;
use fluvio_spu_schema::server::fetch_offset::FetchOffsetsRequest;
use fluvio_spu_schema::server::fetch_offset::FetchOffsetPartitionResponse;
use fluvio_spu_schema::server::stream_fetch::DefaultStreamFetchRequest;
use dataplane::fetch::DefaultFetchRequest;
use dataplane::fetch::FetchPartition;
use dataplane::fetch::FetchableTopic;
use dataplane::fetch::FetchablePartitionResponse;
use dataplane::ReplicaKey;
use dataplane::record::RecordSet;
use dataplane::PartitionOffset;
use crate::FluvioError;
use crate::params::FetchOffset;
use crate::params::FetchLogOption;
use crate::client::SerialFrame;
use crate::spu::SpuPool;

/// consume message from replica leader
pub struct Consumer {
    replica: ReplicaKey,
    pool: SpuPool,
}

impl Consumer {
    pub(crate) fn new(replica: ReplicaKey, pool: SpuPool) -> Self {
        Self { replica, pool }
    }

    /// fetch logs once
    pub async fn fetch_logs_once(
        &mut self,
        offset_option: FetchOffset,
        option: FetchLogOption,
    ) -> Result<FetchablePartitionResponse<RecordSet>, FluvioError> {
        debug!(
            "starting fetch log once: {:#?} from replica: {}",
            offset_option, self.replica,
        );

        let mut leader = self.pool.create_serial_socket(&self.replica).await?;

        debug!("found spu leader {}", leader);

        let offset = calc_offset(&mut leader, &self.replica, offset_option).await?;

        let partition = FetchPartition {
            partition_index: self.replica.partition,
            fetch_offset: offset,
            max_bytes: option.max_bytes,
            ..Default::default()
        };

        let topic_request = FetchableTopic {
            name: self.replica.topic.to_owned(),
            fetch_partitions: vec![partition],
        };

        let fetch_request = DefaultFetchRequest {
            topics: vec![topic_request],
            isolation_level: option.isolation,
            max_bytes: option.max_bytes,
            ..Default::default()
        };

        let response = leader.send_receive(fetch_request).await?;

        debug!("received fetch logs for {}", self.replica);

        if let Some(partition_response) =
            response.find_partition(&self.replica.topic, self.replica.partition)
        {
            debug!(
                "found partition response with: {} batches: {} bytes",
                partition_response.records.batches.len(),
                bytes_count(&partition_response.records)
            );
            Ok(partition_response)
        } else {
            Err(FluvioError::PartitionNotFound(
                self.replica.topic.clone(),
                self.replica.partition,
            ))
        }
    }

    /// fetch logs as stream
    /// this will fetch continously
    pub async fn fetch_logs_as_stream(
        &mut self,
        offset_option: FetchOffset,
        option: FetchLogOption,
    ) -> Result<AsyncResponse<DefaultStreamFetchRequest>, FluvioError> {
        debug!(
            "starting fetch log once: {:#?} from replica: {}",
            offset_option, self.replica,
        );

        let mut serial_socket = self.pool.create_serial_socket(&self.replica).await?;
        debug!("created serial socket {}", serial_socket);
        let offset = calc_offset(&mut serial_socket, &self.replica, offset_option).await?;
        drop(serial_socket);

        let stream_request = DefaultStreamFetchRequest {
            topic: self.replica.topic.to_owned(),
            partition: self.replica.partition,
            fetch_offset: offset,
            isolation: option.isolation,
            max_bytes: option.max_bytes,
            ..Default::default()
        };

        self.pool.create_stream(&self.replica, stream_request).await
    }
}

async fn fetch_offsets<F: SerialFrame>(
    client: &mut F,
    replica: &ReplicaKey,
) -> Result<FetchOffsetPartitionResponse, FluvioError> {
    debug!("fetching offset for replica: {}", replica);

    let response = client
        .send_receive(FetchOffsetsRequest::new(
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
            debug!("replica: {}, fetch offset: {}", replica, partition_response);
            Ok(partition_response)
        }
        None => Err(IoError::new(
            ErrorKind::InvalidData,
            format!("no replica offset for: {}", replica),
        )
        .into()),
    }
}

/// depends on offset option, calculate offset

async fn calc_offset<F: SerialFrame>(
    client: &mut F,
    replica: &ReplicaKey,
    offset: FetchOffset,
) -> Result<i64, FluvioError> {
    Ok(match offset {
        FetchOffset::Offset(inner_offset) => inner_offset,
        FetchOffset::Earliest(relative_offset) => {
            let offsets = fetch_offsets(client, replica).await?;
            offsets.start_offset() + relative_offset.unwrap_or(0)
        }
        FetchOffset::Latest(relative_offset) => {
            let offsets = fetch_offsets(client, replica).await?;
            offsets.last_stable_offset() - relative_offset.unwrap_or(0)
        }
    })
}

/// compute total bytes in record set
fn bytes_count(records: &RecordSet) -> usize {
    records
        .batches
        .iter()
        .map(|batch| {
            batch
                .records
                .iter()
                .map(|record| record.value.len())
                .sum::<usize>()
        })
        .sum()
}

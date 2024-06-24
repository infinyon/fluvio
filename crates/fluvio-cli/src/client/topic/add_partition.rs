//!
//! # Increment partition of a Topic
//!
//! CLI tree to increment the number of partitions of a topic.
//!
use clap::Parser;
use anyhow::{anyhow, Result};
use comfy_table::{Cell, Row, Table};
use tokio::select;
use futures::StreamExt;

use fluvio_types::ReplicaMap;
use fluvio_future::timer::sleep;
use fluvio_sc_schema::topic::{AddPartition, TopicSpec, UpdateTopicAction};
use fluvio::{Fluvio, FluvioAdmin};

/// Option for Listing Partition
#[derive(Debug, Parser)]
pub struct AddPartitionOpt {
    /// Topic name
    topic: String,
    /// Number of partitions to add
    #[arg(short, long, default_value = "1")]
    count: i32,
}

const CHECK_NEW_PARTITIONS_TIMEOUT_MS: u64 = 10000;

impl AddPartitionOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<()> {
        let admin = fluvio.admin().await;

        let topic = admin
            .list::<TopicSpec, _>(vec![self.topic.clone()])
            .await?
            .into_iter()
            .find(|t| t.name == self.topic)
            .ok_or_else(|| anyhow!("topic \"{}\" not found", self.topic))?;

        let replica_map_before = topic.status.replica_map.clone();
        let replica_map_after = self
            .add_and_watch_new_partitions(&replica_map_before, &admin)
            .await?;
        let diff = replica_map_after
            .into_iter()
            .filter(|(k, v)| {
                let before = replica_map_before.get(k);
                match before {
                    Some(before) => *before != *v,
                    None => true,
                }
            })
            .collect::<Vec<_>>();

        println!("added new partitions to topic: \"{}\"", self.topic);
        println!("{}", display_new_partitions_spu_table(diff));

        Ok(())
    }

    async fn add_and_watch_new_partitions(
        &self,
        replica_map_before: &ReplicaMap,
        admin: &FluvioAdmin,
    ) -> Result<ReplicaMap> {
        let request = AddPartition {
            count: self.count as u32,
        };

        let action = UpdateTopicAction::AddPartition(request);
        let mut partition_stream = admin.watch::<TopicSpec>().await?;
        let mut connected = false;
        loop {
            select! {
                event = partition_stream.next() => {
                    if let Some(Ok(event)) = event {
                        if !connected {
                            let _ = admin
                                .update::<TopicSpec>(self.topic.clone(), action.clone())
                                .await;
                            connected = true;
                        }

                        for change in event.inner().changes.iter() {
                            let replica_map_after = &change.content.status.replica_map;
                            if replica_map_after.len() == replica_map_before.len() + self.count as usize {
                                return Ok(replica_map_after.clone());
                            }
                        }
                    }
                }
                _ = sleep(std::time::Duration::from_millis(CHECK_NEW_PARTITIONS_TIMEOUT_MS)) => {
                    return Err(anyhow!("response timeout exceeded"))
                },
            }
        }
    }
}

fn display_new_partitions_spu_table(partitions_spus: Vec<(u32, Vec<i32>)>) -> String {
    let values = partitions_spus
        .into_iter()
        .map(|(partition, replicas)| {
            let spu = replicas.first().expect("no replicas found");
            Row::from(vec![
                Cell::new(partition.to_string()),
                Cell::new(spu.to_string()),
            ])
        })
        .collect::<Vec<_>>();

    let header = Row::from([Cell::new("PARTITION"), Cell::new("SPU")]);
    let mut table_list = Table::new();
    table_list.set_header(header);
    for value in values {
        table_list.add_row(value);
    }
    table_list.load_preset(comfy_table::presets::NOTHING);
    table_list.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluvio_future::test_async;
    use fluvio::FluvioError;

    #[test_async]
    async fn test_add_partition_display() -> Result<(), FluvioError> {
        let partitions_spus = vec![(1, vec![5001]), (2, vec![5002])];
        let partition_spu_table = display_new_partitions_spu_table(partitions_spus);

        assert_eq!(
            partition_spu_table,
            r#" PARTITION  SPU  
 1          5001 
 2          5002 "#
        );

        Ok(())
    }
}

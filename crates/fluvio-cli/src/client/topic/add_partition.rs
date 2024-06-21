//!
//! # Increment partition of a Topic
//!
//! CLI tree to increment the number of partitions of a topic.
//!
use clap::Parser;
use anyhow::Result;
use comfy_table::{Cell, Row, Table};
use tokio::select;
use futures::StreamExt;

use fluvio_future::timer::sleep;
use fluvio_sc_schema::topic::{AddPartition, TopicSpec, UpdateTopicAction};
use fluvio::Fluvio;

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

        let request = AddPartition {
            count: self.count as u32,
        };

        let action = UpdateTopicAction::AddPartition(request);
        let topic_before = admin
            .list::<TopicSpec, _>(vec![self.topic.clone()])
            .await?
            .first()
            .expect("topic not found")
            .clone();
        let replica_map_before = topic_before.status.replica_map;

        let mut partition_stream = admin.watch::<TopicSpec>().await?;
        let timeout = sleep(std::time::Duration::from_millis(
            CHECK_NEW_PARTITIONS_TIMEOUT_MS,
        ));

        let send_new_partitions_and_watch_them = async {
            let mut connected = false;
            loop {
                if let Some(Ok(event)) = partition_stream.next().await {
                    if !connected {
                        let _ = admin
                            .update::<TopicSpec>(self.topic.clone(), action.clone())
                            .await;
                        connected = true;
                    }

                    for change in event.inner().changes.iter() {
                        let replica_map_after = &change.content.status.replica_map;
                        if replica_map_after.len() == replica_map_before.len() + self.count as usize
                        {
                            return replica_map_after.clone();
                        }
                    }
                }
            }
        };

        select! {
            replica_map_after = send_new_partitions_and_watch_them => {
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

                display(diff, &self.topic);

            },
            _ = timeout => {
                println!("response timeout exceeded");
            },
        }

        Ok(())
    }
}

fn display(diff: Vec<(u32, Vec<i32>)>, topic: &str) {
    let values = diff
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
    let out: Vec<String> = table_list.lines().collect();
    println!("added new partitions to topic: \"{}\"", topic);
    println!("{}", out.join("\n"));
}

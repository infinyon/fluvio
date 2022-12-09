mod common;
mod source;

use std::path::PathBuf;

use clap::Parser;
use fluvio::{TopicProducer, RecordKey};
use fluvio_connector_package::config::ConnectorConfig;
use futures::StreamExt;
use anyhow::Result;

use crate::{source::TestJsonSource, common::Source};

#[async_std::main]
async fn main() -> Result<()> {
    println!("Hello, world!");

    let opts = Opt::parse();
    println!(
        "Reading config file from: {}",
        opts.config.to_string_lossy()
    );

    let config = ConnectorConfig::from_file(opts.config.as_path())?;
    println!("{:#?}", config);

    println!("starting processing");
    let producer = common::create_producer(&config).await?;
    start(config, producer).await?;
    Ok(())
}

#[derive(Debug, Parser)]
pub struct Opt {
    #[clap(short, long, value_name = "PATH")]
    config: PathBuf,
}

async fn start(config: ConnectorConfig, producer: TopicProducer) -> anyhow::Result<()> {
    let source = TestJsonSource::new(&config)?;
    let mut stream = source.connect(None).await?;
    while let Some(item) = stream.next().await {
        println!("producing a value: {}", &item);
        producer.send(RecordKey::NULL, item).await?;
    }
    Ok(())
}

use std::path::PathBuf;

use clap::Parser;
use fluvio_connector_package::config::ConnectorConfig;

fn main() {
    println!("Hello, world!");

    let opts = Opt::parse();
    println!(
        "Reading config file from: {}",
        opts.config.to_string_lossy()
    );

    let config =
        ConnectorConfig::from_file(opts.config.as_path()).expect("unable to read config file");
    println!("{:#?}", config);

    logic::read(config).expect("unable to read config");
}

#[derive(Debug, Parser)]
pub struct Opt {
    #[clap(short, long, value_name = "PATH")]
    config: PathBuf,
}

/// this is connector specific logic
mod logic {

    use serde::Deserialize;
    use anyhow::Result;

    use fluvio_connector_package::config::ConnectorConfig;

    #[derive(Debug, Default, Deserialize)]
    pub(crate) struct TestParameter {
        interval: u32,
        template: String,
    }

    /// read from connector config
    pub(crate) fn read(config: ConnectorConfig) -> Result<()> {
        // read from connector config to parameters
        let interval = match config.parameters.get("interval") {
            Some(value) => value.as_u32()?,
            None => anyhow::bail!("interval not found"),
        };
        let template = match config.parameters.get("template") {
            Some(value) => value.as_string()?,
            None => anyhow::bail!("template not found"),
        };

        let parameters = TestParameter { interval, template };

        println!("interval: {}", parameters.interval);
        println!("template: {}", parameters.template);

        // connect to fluvio

        // wait interval and generate to produce in the loop

        Ok(())
    }
}

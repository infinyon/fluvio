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
}

#[derive(Debug, Parser)]
pub struct Opt {
    #[clap(short, long, value_name = "PATH")]
    config: PathBuf,
}

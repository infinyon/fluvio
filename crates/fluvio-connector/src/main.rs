mod error;
mod connector;
mod config;

use error::ConnectorError;
use connector::ManagedConnectorCmd as ConnectorOpts;

use structopt::StructOpt;
use fluvio_extension_common::PrintTerminal;
use std::sync::Arc;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), ConnectorError> {
    fluvio_future::subscriber::init_tracer(None);
    let opts = ConnectorOpts::from_args();
    let out = Arc::new(PrintTerminal::new());
    let fluvio = fluvio::Fluvio::connect()
        .await
        .expect("Failed to connect to fluvio");

    let _ = opts.process(out, &fluvio).await?;

    Ok(())
}

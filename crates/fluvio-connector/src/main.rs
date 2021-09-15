mod error;
mod connector;
mod config;

use error::ConnectorError;
use connector::ManagedConnectorCmd as ConnectorOpts;

use structopt::StructOpt;
use fluvio_extension_common::PrintTerminal;
use std::sync::Arc;
use fluvio_future::task::run_block_on;

fn main() -> Result<(), ConnectorError> {
    fluvio_future::subscriber::init_tracer(None);
    let opts = ConnectorOpts::from_args();
    let out = Arc::new(PrintTerminal::new());

    let _ = run_block_on(opts.process(out))?;

    Ok(())
}

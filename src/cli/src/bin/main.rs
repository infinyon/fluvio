use structopt::StructOpt;
use color_eyre::eyre::Result;
use fluvio_cli::Root;
use fluvio_future::task::run_block_on;

fn main() -> Result<()> {
    fluvio_future::subscriber::init_tracer(None);
    color_eyre::install()?;

    let args: Vec<_> = std::env::args().collect();
    let root: Root = Root::from_iter(args);
    run_block_on(root.process())?;

    Ok(())
}

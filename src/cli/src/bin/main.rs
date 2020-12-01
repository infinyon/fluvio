use structopt::StructOpt;
use color_eyre::eyre::Result;
use fluvio_cli::{Root, CliError, HelpOpt};
use fluvio_future::task::run_block_on;

fn main() -> Result<()> {
    fluvio_future::subscriber::init_tracer(None);
    color_eyre::config::HookBuilder::blank()
        .display_env_section(false)
        .install()?;
    let args = std::env::args();
    if args.len() < 2 {
        HelpOpt {}.process()?;
    } else {
        let root: Root = Root::from_args();
        run_block_on(root.process()).map_err(CliError::into_report)?;
    }
    Ok(())
}

use structopt::StructOpt;
use color_eyre::eyre::Result;
use fluvio_cli::{Root, print_help_hack};
use fluvio_future::task::run_block_on;

fn main() -> Result<()> {
    fluvio_future::subscriber::init_tracer(None);
    color_eyre::config::HookBuilder::blank()
        .display_env_section(false)
        .install()?;

    print_help_hack()?;
    let root: Root = Root::from_args();

    // If the CLI comes back with an error, attempt to handle it
    if let Err(e) = run_block_on(root.process()) {
        e.print()?;
        std::process::exit(1);
    }

    Ok(())
}

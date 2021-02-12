use structopt::StructOpt;
use color_eyre::eyre::Result;
use fluvio_cli::{Root, HelpOpt};
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
        match e.try_handle() {
            Ok(exit_code) => std::process::exit(exit_code),
            Err(unhandled) => return Err(unhandled.into_report()),
        }
    }

    Ok(())
}

fn print_help_hack() -> Result<()> {
    let mut args = std::env::args();
    if args.len() < 2 {
        HelpOpt {}.process()?;
        std::process::exit(0);
    } else if let Some(first_arg) = args.nth(1) {
        if vec!["-h", "--help", "help"].contains(&&first_arg.as_str()) {
            HelpOpt {}.process()?;
            std::process::exit(0);
        }
    }
    Ok(())
}

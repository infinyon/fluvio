use structopt::StructOpt;
use color_eyre::eyre::Result;
use fluvio_cli::{Root, CliError, HelpOpt};
use fluvio_future::task::run_block_on;

fn main() -> Result<()> {
    fluvio_future::subscriber::init_tracer(None);
    color_eyre::config::HookBuilder::blank()
        .display_env_section(false)
        .install()?;
    print_help_hack()?;
    let root: Root = Root::from_args();
    let result = run_block_on(root.process());

    // If an error was handled gracefully, we still want to exit with the right code
    if let Err(CliError::ExitWithCode(code)) = result {
        std::process::exit(code);
    }

    result.map_err(CliError::into_report)?;
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

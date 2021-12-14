use structopt::StructOpt;
use color_eyre::eyre::Result;
use fluvio_cli::{Root, HelpOpt};
use fluvio_future::task::run_block_on;
use fluvio_cli::cli_config::FluvioChannelConfig;
use std::env::current_exe;

fn main() -> Result<()> {
    fluvio_future::subscriber::init_tracer(None);
    color_eyre::config::HookBuilder::blank()
        .display_env_section(false)
        .install()?;

    // Check on channel
    let channel_config_path = FluvioChannelConfig::default_config_location();

    let channel = if FluvioChannelConfig::exists(&channel_config_path) {
        //println!("Config file exists @ {:#?}", &channel_config_path);
        FluvioChannelConfig::from_file(channel_config_path)?
        //FluvioChannelConfig::default()
    } else {
        // Default to stable channel behavior
        FluvioChannelConfig::default()
    };

    //println!("{:#?}", &channel);

    // Verify if the current binary is the same as the channel we're using
    let current_exe = current_exe()?;

    //println!("Current exe: {:?}", &current_exe);
    //println!("Config current exe: {:?}", &channel.current_exe());

    if let Some(exe) = channel.current_exe() {
        if exe == current_exe {
            println!("You're using the stable exe");
            println!("DEBUG: {:#?}", channel);
        } else {
            // If not, exec the correct binary
            println!("You're not using the stable exe");
            println!("DEBUG: {:#?}", channel);
        }
    } else {
        unreachable!("Default channel exe should be initialized")
    }

    print_help_hack()?;
    let root: Root = Root::from_args();

    // If the CLI comes back with an error, attempt to handle it
    if let Err(e) = run_block_on(root.process()) {
        e.print()?;
        std::process::exit(1);
    }

    Ok(())
}

fn print_help_hack() -> Result<()> {
    let mut args = std::env::args();
    if args.len() < 2 {
        HelpOpt {}.process()?;
        std::process::exit(0);
    } else if let Some(first_arg) = args.nth(1) {
        if vec!["-h", "--help", "help"].contains(&first_arg.as_str()) {
            HelpOpt {}.process()?;
            std::process::exit(0);
        }
    }
    Ok(())
}

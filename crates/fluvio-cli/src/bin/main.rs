use structopt::StructOpt;
use color_eyre::eyre::Result;
use fluvio_cli::{Root, HelpOpt, cli_config::CliChannelName};
use fluvio_future::task::run_block_on;
use fluvio_cli::cli_config::FluvioChannelConfig;
use std::{env::current_exe, os::unix::prelude::CommandExt};
use std::ffi::OsString;
use tracing::debug;

fn main() -> Result<()> {
    fluvio_future::subscriber::init_tracer(None);
    color_eyre::config::HookBuilder::blank()
        .display_env_section(false)
        .install()?;

    print_help_hack()?;
    let root: Root = Root::from_args();

    // Check on channel
    // But make sure we don't prevent any commands to change channel
    // TODO: Add skip_channel_check to prevent an exec

    #[cfg(not(target_os = "windows"))]
    if !root.skip_channel_check() {
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

        debug!("channel: {:#?}", channel);
        let _do_change_binary = if let Some(exe) = channel.current_exe() {
            if exe != current_exe {
                // If not, exec the correct binary
                debug!("You're NOT using the configured current_channel binary");
                // TODO:
                // If we're CLIChannelName::Stable or CLIChannelName::Latest, then use that
                // If we're CLIChannelName::Dev, do nothing
                if channel.current_channel() == CliChannelName::Dev {
                    debug!("You're in Developer mode");
                    false
                } else {
                    debug!("Will exec to binary @ {:?}", exe);

                    // Re-build the args list to pass onto exec'ed process
                    let mut args: Vec<OsString> = std::env::args_os().collect();
                    if !args.is_empty() {
                        args.remove(0);
                    }

                    let mut proc = std::process::Command::new(exe);
                    proc.args(args);

                    let _err = proc.exec();
                    true
                }
            } else {
                debug!("You're using the configured current_channel binary");
                false
            }
        } else {
            panic!("Default channel exe should be initialized")
        };
    }
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

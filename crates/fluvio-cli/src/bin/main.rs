use structopt::StructOpt;
use color_eyre::eyre::Result;
use fluvio_cli::{Root, HelpOpt};
use fluvio_future::task::run_block_on;
use std::env::current_exe;
use std::ffi::OsString;
use tracing::debug;
use std::process::Stdio;
use fluvio_cli::cli_config::channel::{CliChannelName, FluvioChannelConfig, is_fluvio_bin_in_std_dir};
#[cfg(not(target_os = "windows"))]
use std::os::unix::prelude::CommandExt;
#[cfg(target_os = "windows")]
use std::os::windows::prelude::CommandExt;

// TODO: This needs to support more than the 3 main channels
// Pass overrides for extension dir, image name/path, image pattern format

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

    // Verify if the current binary is running in the "official" location
    // If we're not in the fluvio directory then
    // assume dev mode (i.e., do not exec to other binaries)
    let current_exe = current_exe()?;

    if is_fluvio_bin_in_std_dir(&current_exe) && !root.skip_channel_check() {
        let channel_config_path = FluvioChannelConfig::default_config_location();

        let channel = if FluvioChannelConfig::exists(&channel_config_path) {
            //println!("Config file exists @ {:#?}", &channel_config_path);
            FluvioChannelConfig::from_file(channel_config_path)?
            //FluvioChannelConfig::default()
        } else {
            // Default to stable channel behavior
            FluvioChannelConfig::default()
        };

        debug!("channel: {:#?}", channel);

        // Switch binaries
        if let Some(exe) = channel.current_exe() {
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

                    // Handle pipes

                    let mut proc = std::process::Command::new(exe);
                    proc.args(args);
                    proc.stdin(Stdio::inherit());
                    proc.stdout(Stdio::inherit());
                    proc.stderr(Stdio::inherit());

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
    } else {
        debug!("Fluvio bin not in standard install location. Assuming dev channel")
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

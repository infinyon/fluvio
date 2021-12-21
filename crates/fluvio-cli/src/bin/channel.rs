//use structopt::StructOpt;
use color_eyre::eyre::{Result, eyre};
use structopt::StructOpt;

use fluvio_cli::{Root, HelpOpt};
use std::env::current_exe;
use std::ffi::OsString;
use tracing::debug;
use std::process::Stdio;
use fluvio_cli::channel::{FluvioChannelConfig, FluvioChannelInfo};
#[cfg(not(target_os = "windows"))]
use std::os::unix::prelude::CommandExt;
#[cfg(target_os = "windows")]
use std::io::{self, Write};
use cfg_if::cfg_if;

// Create custom channels
// Support Version number release channels

// If possible, accept no args, or find a way to squash in the Fluvio ones that we'll forward
// would like if `fluvio --help` was handled by the channel binary

// OR act as a Fluvio frontend only if this binary is named `fluvio`

fn main() -> Result<()> {
    fluvio_future::subscriber::init_tracer(None);

    debug!("Check if running as fluvio frontend");
    // Verify if the current binary is running in the "official" location
    // If we're not in the fluvio directory then
    // assume dev mode (i.e., do not exec to other binaries)
    let current_exe = current_exe()?;

    let is_frontend = if let Some(file) = current_exe.file_name() {
        let file_name = file.to_str().unwrap_or_default();

        if ["fluvio", "fluvio.exe"].contains(&file_name) {
            // Check on the name this binary was called. If it is `fluvio` be in transparent frontend-mode
            debug!("Binary is named `{}` - Frontend mode", file_name);
            true
        } else {
            // If development-mode, use the `--help` output from `fluvio-channel`
            debug!("Binary is named `{}` - Development mode", file_name);
            false
        }
    } else {
        unreachable!(
            "current_exe is a directory path instead of a binary: {:#?}",
            current_exe
        );
    };

    // Pick a fluvio binary
    //
    //if is_frontend {

    //}

    // open a config file
    // if one doesn't exist, create it
    // (TODO: Make that location configurable)
    // initialize with stable, latest, dev channels
    let channel_config_path = FluvioChannelConfig::default_config_location();
    let _channel = if FluvioChannelConfig::exists(&channel_config_path) {
        //println!("Config file exists @ {:#?}", &channel_config_path);
        FluvioChannelConfig::from_file(channel_config_path)?
        //FluvioChannelConfig::default()
    } else {
        // Default to stable channel behavior
        FluvioChannelConfig::default()
    };

    //// Read in args
    //// Handle what is `fluvio-channel` specific
    print_help_hack()?;
    let root: Root = Root::from_args();

    // //

    // Check on channel
    // But make sure we don't prevent any commands to change channel

    let channel = if is_frontend && !root.skip_channel_check() {
        // Look for channel config
        // TODO: Let this be configurable
        let channel_config_path = FluvioChannelConfig::default_config_location();

        let maybe_channel_config = if FluvioChannelConfig::exists(&channel_config_path) {
            //println!("Config file exists @ {:#?}", &channel_config_path);
            Some(FluvioChannelConfig::from_file(channel_config_path)?)
            //FluvioChannelConfig::default()
        } else {
            // If the channel config doesn't exist, we will create one and initialize with defaults
            // Default to stable channel behavior
            //FluvioChannelConfig::default()
            None
        };

        debug!("channel_config: {:#?}", maybe_channel_config);

        // Return the channel info for fluvio location
        let channel_info = if let Some(channel_config) = maybe_channel_config {
            let channel = channel_config.current_channel();

            channel_config
                .config()
                .channel()
                .get(&channel)
                .ok_or_else(|| eyre!("Channel info not found"))?
                .to_owned()
        } else {
            // Write the channel config for the first time
            let stable_info = FluvioChannelInfo::stable_channel();

            let mut default_config = FluvioChannelConfig::default();

            default_config.insert_channel("stable".to_string(), stable_info.clone())?;
            default_config.set_current_channel("stable".to_string())?;
            default_config.save()?;

            stable_info
        };

        channel_info
        //// Run the channel binary
        //if let Some(exe) = channel.current_exe() {
        //    if exe != current_exe {
        //        // If not, exec the correct binary
        //        debug!("You're NOT using the configured current_channel binary");
        //        // TODO:
        //        // If we're CLIChannelName::Stable or CLIChannelName::Latest, then use that
        //        // If we're CLIChannelName::Dev, do nothing
        //        if channel.current_channel() == CliChannelName::Dev {
        //            debug!("You're in Developer mode");
        //            false
        //        } else {
        //            debug!("Will exec to binary @ {:?}", exe);

        //            // Re-build the args list to pass onto exec'ed process
        //            let mut args: Vec<OsString> = std::env::args_os().collect();
        //            if !args.is_empty() {
        //                args.remove(0);
        //            }

        //            // Handle pipes
        //            let mut proc = std::process::Command::new(exe);
        //            proc.args(args);
        //            proc.stdin(Stdio::inherit());
        //            proc.stdout(Stdio::inherit());
        //            proc.stderr(Stdio::inherit());

        //            cfg_if! {
        //                if #[cfg(not(target_os = "windows"))] {
        //                    let _err = proc.exec();
        //                } else {
        //                    // TODO: This needs to be sure to add .exe to filename
        //                    // Handle unwrap()
        //                    let output = proc.output()?;
        //                    io::stdout().write_all(&output.stdout).unwrap();
        //                    io::stderr().write_all(&output.stderr).unwrap();
        //                }
        //            }

        //            true
        //        }
        //    } else {
        //        debug!("You're using the configured current_channel binary");
        //        false
        //    }
        //} else {
        //    panic!("Default channel exe should be initialized")
        //};
    } else {
        debug!("Fluvio bin not in standard install location. Assuming dev channel");
        FluvioChannelInfo::dev_channel()
    };

    let exe = channel.get_binary_path();

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

    cfg_if! {
        if #[cfg(not(target_os = "windows"))] {
            let _err = proc.exec();
        } else {
            // TODO: This needs to be sure to add .exe to filename
            // Handle unwrap()
            let output = proc.output()?;
            io::stdout().write_all(&output.stdout).unwrap();
            io::stderr().write_all(&output.stderr).unwrap();
        }
    }

    // If it doesn't exist do nothing, default channel is Stable

    // If it does exist, look up the current channel
    // Load the the Channel Info

    // Fluvio Binary: ~/.fluvio/bin/fluvio (default)
    // Extensions directory: ~/.fluvio/extensions (default)
    // K8 image: infinyon/fluvio (default)
    // Image tag format: {version (default), version-git, git}

    // Fluvio binary resolution order:
    // From channel config
    // In PATH
    // In default Fluvio directory ($HOME/.fluvio/bin)
    // In current directory
    // From FLUVIO_BIN env var

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

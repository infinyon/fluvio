use std::env;
//use structopt::StructOpt;
use color_eyre::eyre::{Result, eyre};
use structopt::StructOpt;

use fluvio_cli::{Root, RootCmd, HelpOpt, channel::ImageTagStrategy};
use std::env::current_exe;
use std::ffi::OsString;
use tracing::debug;
use std::process::Stdio;
use fluvio_cli::channel::{FluvioChannelConfig, FluvioChannelInfo, FluvioBinVersion};
#[cfg(not(target_os = "windows"))]
use std::os::unix::prelude::CommandExt;
#[cfg(target_os = "windows")]
use std::io::{self, Write};
use cfg_if::cfg_if;
use fluvio_future::task::run_block_on;

const IS_FLUVIO_EXEC_LOOP: &str = "IS_FLUVIO_EXEC_LOOP";

// `fluvio-channel` is a Fluvio frontend to support release channels.
// It is intended to be installed at `~/.fluvio/bin/fluvio`
fn main() -> Result<()> {
    fluvio_future::subscriber::init_tracer(None);

    let current_exe = current_exe()?;
    debug!("Check if channel exec is in a loop");
    if let Ok(channel_info_str) = env::var(IS_FLUVIO_EXEC_LOOP) {
        // Expecting this string to be in the form
        // "channel_name,/path/of/the/channel/binary/exec"
        let channel: Vec<&str> = channel_info_str.split(',').collect();
        if channel.len() == 2 {
            eprintln!(
                "Couldn't find Fluvio channel binary '{}' at '{}'",
                channel[0], channel[1]
            );
        } else {
            eprintln!("Couldn't find Fluvio channel binary (Unexpected error formatting (raw output): {})", channel_info_str);
        }
        panic!("Exec loop detected");
    }

    debug!("Check if running as fluvio frontend");
    // Verify if the current binary is running in the "official" location
    // If we're not in the fluvio directory then
    // assume dev mode (i.e., do not exec to other binaries)
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

    // open a config file
    // if one doesn't exist, we'll eventually create it before exec
    // (TODO: Make that location overridable (env var / optional flag))
    // initialize with stable, latest, dev channels
    let channel_config_path = FluvioChannelConfig::default_config_location();
    let _channel = if FluvioChannelConfig::exists(&channel_config_path) {
        //println!("Config file exists @ {:#?}", &channel_config_path);
        FluvioChannelConfig::from_file(channel_config_path)?
    } else {
        // Default to stable channel behavior
        FluvioChannelConfig::default()
    };

    debug!("About to process args");

    // Read in args
    print_help_hack()?;
    let root: Root = Root::from_args();

    debug!("After args: {:#?}", &root);

    // We need to make sure we always have a stable interface for switching channels
    // So we're going to handle the `fluvio version <channel stuff>` commands if we're not in development mode
    if let RootCmd::Version(version_opt) = root.command.clone() {
        debug!("Found a version command");

        match version_opt.cmd {
            Some(channel_cmd) => {
                if let Err(e) = run_block_on(channel_cmd.process(root.opts.target)) {
                    println!("{}", e.into_report());
                    std::process::exit(1);
                }
                std::process::exit(0);
            }
            None => debug!("pass Version command w/o subcommands to fluvio binary"),
        }
    } else {
        debug!("Command was not version");
    }

    // //

    // Check on channel via channel config file
    let (channel_name, channel) = if is_frontend && !root.skip_channel_check() {
        // Look for channel config
        // TODO: Let this be configurable
        let channel_config_path = FluvioChannelConfig::default_config_location();

        let maybe_channel_config = if FluvioChannelConfig::exists(&channel_config_path) {
            //println!("Config file exists @ {:#?}", &channel_config_path);
            Some(FluvioChannelConfig::from_file(channel_config_path)?)
        } else {
            // If the channel config doesn't exist, we will create one and initialize with defaults
            None
        };

        debug!("channel_config: {:#?}", maybe_channel_config);

        // Return the channel info for fluvio location
        // Write config file to disk if it doesn't already exist
        let (channel, channel_info) = if let Some(channel_config) = maybe_channel_config {
            let channel = channel_config.current_channel();

            let channel_info = channel_config
                .config()
                .channel()
                .get(&channel)
                .ok_or_else(|| eyre!("Channel info not found"))?
                .to_owned();

            (channel, channel_info)
        } else {
            // Write the channel config for the first time

            let mut default_config = FluvioChannelConfig::default();

            // If we know we've been called by the installer, then let's add that channel info
            if env::var("FLUVIO_BOOTSTRAP").is_ok() {
                let initial_channel = env::var("CHANNEL_BOOTSTRAP")?;

                // parse a version from the channel name
                let image_tag_strategy = match FluvioBinVersion::parse(&initial_channel)? {
                    FluvioBinVersion::Stable => ImageTagStrategy::Version,
                    FluvioBinVersion::Latest => ImageTagStrategy::VersionGit,
                    FluvioBinVersion::Tag(_) => ImageTagStrategy::Version,
                    FluvioBinVersion::Dev => ImageTagStrategy::Git,
                };

                // Create a new channel
                let new_channel_info =
                    FluvioChannelInfo::new_channel(&initial_channel, image_tag_strategy);

                default_config.insert_channel(initial_channel.clone(), new_channel_info.clone())?;
                default_config.set_current_channel(initial_channel.clone())?;
                default_config.save()?;

                (initial_channel, new_channel_info)
            } else {
                let stable_info = FluvioChannelInfo::stable_channel();
                default_config.insert_channel("stable".to_string(), stable_info.clone())?;
                default_config.set_current_channel("stable".to_string())?;
                default_config.save()?;

                ("stable".to_string(), stable_info)
            }
        };

        (channel, channel_info)
    } else {
        debug!("Fluvio bin not in standard install location. Assuming dev channel");
        ("dev".to_string(), FluvioChannelInfo::dev_channel())
    };

    let exe = channel.get_binary_path();

    debug!("Will exec to binary @ {:?}", exe);

    // Re-build the args list to pass onto exec'ed process
    let mut args: Vec<OsString> = std::env::args_os().collect();
    if !args.is_empty() {
        args.remove(0);
    }

    // Set the env var we check at the beginning to signal if we're in an exec loop
    // Give channel name and binary location for error message in form: <channel_name>,<channel_path>
    let channel_info = format!("{},{}", channel_name, channel.get_binary_path().display());
    env::set_var(IS_FLUVIO_EXEC_LOOP, channel_info);

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
            let output = proc.output()?;
            io::stdout().write_all(&output.stdout)?;
            io::stderr().write_all(&output.stderr)?;
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
        if vec!["-h", "--help", "help"].contains(&first_arg.as_str()) {
            HelpOpt {}.process()?;
            std::process::exit(0);
        }
    }
    Ok(())
}

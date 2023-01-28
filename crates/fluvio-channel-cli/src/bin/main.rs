use std::env;
use std::str::FromStr;
use std::env::current_exe;
use std::ffi::OsString;
use std::process::Stdio;
#[cfg(not(target_os = "windows"))]
use std::os::unix::prelude::CommandExt;
#[cfg(target_os = "windows")]
use std::io::{self, Write};

use colored::Colorize;
use clap::{Parser, CommandFactory};
use tracing::debug;
use cfg_if::cfg_if;
use anyhow::{anyhow, Result};

use fluvio_future::task::run_block_on;
use fluvio_channel::{
    FluvioChannelConfig, FluvioChannelInfo, FluvioBinVersion, DEV_CHANNEL_NAME, STABLE_CHANNEL_NAME,
};
use fluvio_channel::ImageTagStrategy;
use fluvio_cli_common::{FLUVIO_RELEASE_CHANNEL, FLUVIO_EXTENSIONS_DIR, FLUVIO_IMAGE_TAG_STRATEGY};

use fluvio_channel_cli::cli::create::CreateOpt;
use fluvio_channel_cli::cli::delete::DeleteOpt;
use fluvio_channel_cli::cli::list::ListOpt;
use fluvio_channel_cli::cli::switch::SwitchOpt;

const IS_FLUVIO_EXEC_LOOP: &str = "IS_FLUVIO_EXEC_LOOP";
const FLUVIO_BOOTSTRAP: &str = "FLUVIO_BOOTSTRAP";
const CHANNEL_BOOTSTRAP: &str = "CHANNEL_BOOTSTRAP";
const FLUVIO_FRONTEND: &str = "FLUVIO_FRONTEND";

#[derive(Debug, PartialEq, Parser, Default)]
struct RootOpt {
    #[clap(long)]
    skip_channel_check: bool,
}

#[derive(Debug, PartialEq, Parser, Default)]
#[clap(disable_help_subcommand = true, disable_help_flag = true)]
struct Root {
    #[clap(flatten)]
    opt: RootOpt,
    #[clap(subcommand)]
    command: RootCmd,
}

impl Root {
    pub fn skip_channel_check(&self) -> bool {
        self.opt.skip_channel_check
    }
}

#[derive(Debug, PartialEq, Parser)]
struct ChannelOpt {
    #[clap(subcommand)]
    cmd: Option<ChannelCmd>,
    #[clap(long, short)]
    help: bool,
}

#[derive(Debug, PartialEq, Parser, Clone)]
enum ChannelCmd {
    /// Create a local Fluvio release channel
    Create(CreateOpt),
    /// Delete a local Fluvio release channel
    Delete(DeleteOpt),
    /// List local Fluvio release channels
    List(ListOpt),
    /// Change the active Fluvio release channel
    Switch(SwitchOpt),
}

impl ChannelCmd {
    async fn process(&self) -> Result<()> {
        match self {
            Self::Create(create_opt) => create_opt.process().await,
            Self::Delete(delete_opt) => delete_opt.process().await,
            Self::List(list_opt) => list_opt.process().await,
            Self::Switch(switch_opt) => switch_opt.process().await,
        }
    }
}

#[derive(Debug, PartialEq, Parser)]
#[clap(
    max_term_width = 100,
    disable_version_flag = true,
    // VersionlessSubcommands is now default behaviour. See https://github.com/clap-rs/clap/pull/2831
    // global_setting = AppSettings::DeriveDisplayOrder
)]
enum RootCmd {
    /// Prints help information
    #[clap(hide = true)]
    Help,
    Version(ChannelOpt),

    // This should be the fluvio binary's subcommand
    #[clap(external_subcommand)]
    Other(Vec<String>),
}

impl Default for RootCmd {
    fn default() -> Self {
        RootCmd::Other(Vec::new())
    }
}

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
            eprintln!("Couldn't find Fluvio channel binary (Unexpected error formatting (raw output): {channel_info_str})");
        }
        panic!("Exec loop detected");
    }

    debug!("Check if running as fluvio frontend");
    // Verify if the current binary is running in the "official" location
    // If we're not in the fluvio directory then
    // assume dev mode (i.e., do not exec to other binaries)
    let is_frontend = if let Some(file) = current_exe.file_name() {
        let env_var_set = std::env::var(FLUVIO_FRONTEND)
            .ok()
            .map(|frontend| bool::from_str(&frontend).unwrap_or(false));

        if let Some(env_var) = env_var_set {
            if env_var {
                debug!("Env var set - Frontend mode");
            } else {
                debug!("Env var set - Development mode");
            }

            env_var
        } else {
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
        }
    } else {
        unreachable!(
            "current_exe is a directory path instead of a binary: {:#?}",
            current_exe
        );
    };

    // Now process command line args
    // If we're in frontend mode, we want to pass the help text request

    let fluvio_channel_root = Root::try_parse();

    let channel_cli = if let Ok(channel_cli) = fluvio_channel_root {
        match channel_cli.command {
            RootCmd::Help => {
                debug!("fluvio-channel Help");
                print_help(is_frontend)?;
                std::process::exit(0);
            }
            RootCmd::Version(ref channel_opt) => {
                debug!("fluvio-channel Version");

                if channel_opt.help {
                    let _ = ChannelOpt::command().print_help();
                    println!();
                    std::process::exit(0);
                } else if let Some(subcmd) = &channel_opt.cmd {
                    if let Err(e) = run_block_on(subcmd.process()) {
                        println!("{e}");
                        std::process::exit(1);
                    }

                    std::process::exit(0);
                } else {
                    debug!("Version command should forward to Fluvio binary")
                }
            }
            RootCmd::Other(ref cmd) => {
                debug!("Passing command args to fluvio binary: {:#?}", cmd)
            }
        };
        channel_cli
    } else {
        debug!("Not one of fluvio-channel's subcommands");

        // Re-build the args list to pass onto exec'ed process
        let mut args: Vec<String> = std::env::args().collect();
        if !args.is_empty() {
            args.remove(0);
        }

        Root {
            command: RootCmd::Other(args),
            ..Default::default()
        }
    };

    // Check on channel via channel config file
    let (channel_name, channel) = if is_frontend && !&channel_cli.skip_channel_check() {
        // TODO: Let this be configurable, make the location overridable (env var / optional flag))
        let channel_config_path = FluvioChannelConfig::default_config_location();

        let maybe_channel_config = if FluvioChannelConfig::exists(&channel_config_path) {
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
                .ok_or_else(|| anyhow!("Channel info not found"))?
                .to_owned();

            (channel, channel_info)
        } else {
            // Write the channel config for the first time

            let mut default_config = FluvioChannelConfig::default();

            // If we know we've been called by the installer, then let's add that channel info
            if env::var(FLUVIO_BOOTSTRAP).is_ok() {
                let initial_channel = env::var(CHANNEL_BOOTSTRAP)?;

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
                default_config
                    .insert_channel(STABLE_CHANNEL_NAME.to_string(), stable_info.clone())?;
                default_config.set_current_channel(STABLE_CHANNEL_NAME.to_string())?;
                default_config.save()?;

                (STABLE_CHANNEL_NAME.to_string(), stable_info)
            }
        };

        (channel, channel_info)
    } else {
        debug!("Fluvio bin not in standard install location. Assuming dev channel");
        (
            DEV_CHANNEL_NAME.to_string(),
            FluvioChannelInfo::dev_channel(),
        )
    };

    if let RootCmd::Other(args) = channel_cli.command {
        if args.contains(&"update".to_string())
            && fluvio_channel::is_pinned_version_channel(channel_name.as_str())
        {
            println!(
                    "{}\n{}\n",
                    "Unsupported Feature: The `fluvio update` command is not supported when using a pinned version channel. To use a different version run:".yellow(),
                    "  fluvio version create X.Y.Z\n  fluvio version switch X.Y.Z".italic().yellow()
                );
            std::process::exit(1);
        }
    }

    // Set env vars
    env::set_var(FLUVIO_RELEASE_CHANNEL, channel_name.clone());
    env::set_var(FLUVIO_EXTENSIONS_DIR, channel.extensions.clone());
    env::set_var(
        FLUVIO_IMAGE_TAG_STRATEGY,
        channel.image_tag_strategy.to_string(),
    );

    // On windows, this path should end in `.exe`
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
            let output = proc.output()?;
            io::stdout().write_all(&output.stdout)?;
            io::stderr().write_all(&output.stderr)?;
        }
    }

    Ok(())
}

fn print_help(is_frontend: bool) -> Result<()> {
    if is_frontend {
        debug!("Print Fluvio's help");
        //let mut fluvio_help = FluvioCliRoot::clap();
        //fluvio_help.print_help()?;
    } else {
        debug!("Print Fluvio-channel's help");
        let mut fluvio_channel_help = Root::command();
        fluvio_channel_help.print_help()?;
        std::process::exit(0);
    }

    Ok(())
}

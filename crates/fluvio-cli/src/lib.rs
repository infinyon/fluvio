//! # Root CLI
//!
//! CLI configurations at the top of the tree

mod error;
pub mod client;
pub mod install;
mod profile;
mod version;
mod metadata;
mod render;
pub(crate) mod monitoring;

pub(crate) use error::CliError;
use fluvio_extension_common as common;
pub(crate) const VERSION: &str = include_str!("../../../VERSION");

use fluvio_index::HttpAgent;
use install::update::{
    should_always_print_available_update, check_update_available, prompt_available_update,
};
// list of public export
pub use root::{Root, HelpOpt};
pub use client::TableFormatConfig;

// Checks for an update if channel is latest or ALWAYS_CHECK is set
async fn check_for_channel_update() {
    if should_always_print_available_update() {
        println!("🔍 Checking for new version");
        let agent = HttpAgent::default();
        let update_result = check_update_available(&agent, false).await;
        if let Ok(Some(latest_version)) = update_result {
            prompt_available_update(&latest_version);
        } else {
            println!("✅ fluvio-cli is up to date");
        }
    }
}

mod util {
    use fluvio_spu_schema::Isolation;
    use crate::CliError;

    pub(crate) fn parse_isolation(s: &str) -> Result<Isolation, String> {
        match s {
            "read_committed" | "ReadCommitted" | "readCommitted" | "readcommitted" => {
                Ok(Isolation::ReadCommitted)
            }
            "read_uncommitted" | "ReadUncommitted" | "readUncommitted" | "readuncommitted" => {
                Ok(Isolation::ReadUncommitted)
            }
            _ => Err(format!(
                "unrecognized isolation: {s}. Supported: read_committed (ReadCommitted), read_uncommitted (ReadUncommitted)"
            )),
        }
    }

    pub(crate) fn parse_key_val(s: &str) -> anyhow::Result<(String, String)> {
        let pos = s.find('=').ok_or_else(|| {
            CliError::InvalidArg(format!("invalid KEY=value: no `=` found in `{s}`"))
        })?;
        Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
    }
}

mod root {
    use crate::check_for_channel_update;
    use std::sync::Arc;
    use std::path::PathBuf;
    use std::process::Command;

    use clap::{Parser, Command as ClapCommand, CommandFactory};
    use clap_complete::{generate, Shell};
    use fluvio_benchmark::cli::BenchmarkOpt;
    use tracing::debug;
    use anyhow::Result;

    #[cfg(feature = "k8s")]
    use fluvio_cluster::cli::ClusterCmd;
    use fluvio_cli_common::install::fluvio_extensions_dir;
    use fluvio_channel::{FLUVIO_RELEASE_CHANNEL, LATEST_CHANNEL_NAME};

    use crate::profile::ProfileOpt;
    use crate::install::opts::InstallOpt;
    use crate::client::FluvioCmd;
    use crate::metadata::{MetadataOpt, subcommand_metadata};
    use crate::version::VersionOpt;
    use crate::common::target::ClusterTarget;
    use crate::common::COMMAND_TEMPLATE;
    use crate::common::PrintTerminal;

    /// Fluvio Command Line Interface
    #[derive(Parser, Debug)]
    pub struct Root {
        #[clap(flatten)]
        opts: RootOpt,
        #[clap(subcommand)]
        command: RootCmd,
    }

    impl Root {
        pub async fn process(self) -> Result<()> {
            if command_triggers_update_check(&self.command) {
                tracing::info!("Triggered a Fluvio Update Check");
                check_for_channel_update().await;
            }

            self.command.process(self.opts).await?;
            Ok(())
        }
    }

    #[derive(Parser, Debug)]
    struct RootOpt {
        #[clap(flatten)]
        pub target: ClusterTarget,
    }

    #[derive(Debug, Parser)]
    #[command(
        about = "Fluvio Command Line Interface",
        name = "fluvio",
        help_template = COMMAND_TEMPLATE,
        max_term_width = 100,
        disable_version_flag = true,
        // fluvio consume help would interpret help as topic name, so force -h or --help.
        disable_help_subcommand = true,
        // VersionlessSubcommands is now default behaviour. See https://github.com/clap-rs/clap/pull/2831
        // global_setting = AppSettings::DeriveDisplayOrder
        )]
    enum RootCmd {
        /// All top-level commands that require a Fluvio client are bundled in `FluvioCmd`
        #[command(flatten)]
        #[cfg(feature = "consumer")]
        Fluvio(FluvioCmd),

        /// Run Fluvio benchmarks
        #[command(name = "benchmark", alias = "bench")]
        Benchmark(BenchmarkOpt),

        /// Manage Profiles, which describe linked clusters
        ///
        /// Each Profile describes a particular Fluvio cluster you may be connected to.
        /// This might correspond to Fluvio running on Minikube or in the Cloud.
        /// There is one "active" profile, which determines which cluster all of the
        /// Fluvio CLI commands interact with.
        #[command(name = "profile")]
        Profile(ProfileOpt),

        /// Install or uninstall Fluvio cluster
        ///
        #[cfg(feature = "k8s")]
        #[command(subcommand, name = "cluster")]
        Cluster(Box<ClusterCmd>),

        /// Install Fluvio plugins
        ///
        /// The Fluvio CLI considers any executable with the prefix `fluvio-` to be a
        /// CLI plugin. For example, an executable named `fluvio-foo` in your PATH may
        /// be invoked by running `fluvio foo`.
        ///
        /// This command allows you to install plugins from Fluvio's package registry.
        #[command(name = "install", hide = true)]
        Install(InstallOpt),

        /// Print Fluvio version information
        #[command(name = "version")]
        Version(VersionOpt),

        /// Generate command-line completions for Fluvio
        ///
        /// Run the following two commands to enable fluvio command completions.
        ///
        /// Open a new terminal for the changes to take effect.
        ///
        /// $ fluvio completions bash > ~/fluvio_completions.sh
        /// {n}$ echo "source ~/fluvio_completions.sh" >> ~/.bashrc
        #[command(subcommand, name = "completions")]
        Completions(CompletionCmd),

        /// Generate metadata for Fluvio base CLI
        #[command(name = "metadata", hide = true)]
        Metadata(MetadataOpt),

        #[command(external_subcommand)]
        External(Vec<String>),
    }

    impl RootCmd {
        pub async fn process(self, root: RootOpt) -> Result<()> {
            let out = Arc::new(PrintTerminal::new());

            match self {
                Self::Fluvio(fluvio_cmd) => {
                    fluvio_cmd.process(out, root.target).await?;
                }
                Self::Profile(profile) => {
                    profile.process(out).await?;
                }
                #[cfg(feature = "k8s")]
                Self::Cluster(cluster) => {
                    if let Ok(channel_name) = std::env::var(FLUVIO_RELEASE_CHANNEL) {
                        println!("Current channel: {}", &channel_name);
                    };

                    let version = semver::Version::parse(crate::VERSION).unwrap();
                    cluster.process(out, version, root.target).await?;
                }
                Self::Install(mut install) => {
                    if let Ok(channel_name) = std::env::var(FLUVIO_RELEASE_CHANNEL) {
                        println!("Current channel: {}", &channel_name);

                        if channel_name == LATEST_CHANNEL_NAME {
                            install.develop = true;
                        }
                    };

                    install.process().await?;
                }
                Self::Version(version) => {
                    version.process(root.target).await?;
                }
                Self::Completions(completion) => {
                    completion.process()?;
                }
                Self::Metadata(metadata) => {
                    metadata.process()?;
                }
                Self::Benchmark(bench) => {
                    bench.process().await?;
                }

                Self::External(args) => {
                    process_external_subcommand(args)?;
                }
            }

            Ok(())
        }
    }

    #[derive(Debug, Parser)]
    pub struct HelpOpt {}
    impl HelpOpt {
        pub fn process(self) -> Result<()> {
            let external_commands = subcommand_metadata()?;

            // Add external command definitions to our own clap::Command definition
            let mut app: ClapCommand = Root::command();
            for i in external_commands {
                match i.path.file_name() {
                    Some(file_name) => {
                        app = app.subcommand(
                            ClapCommand::new(
                                file_name
                                    .to_string_lossy()
                                    .strip_prefix("fluvio-")
                                    .unwrap()
                                    .to_owned(),
                            )
                            .about(i.meta.description),
                        );
                    }
                    None => {
                        app = app
                            .subcommand(ClapCommand::new(i.meta.title).about(i.meta.description));
                    }
                }
            }

            // Use clap's help printer, loaded up with external subcommands
            let _ = app.print_help();
            Ok(())
        }
    }

    #[derive(Debug, Parser)]
    struct CompletionOpt {
        #[arg(long, default_value = "fluvio")]
        name: String,
    }

    #[derive(Debug, Parser)]
    enum CompletionCmd {
        /// Generate CLI completions for bash
        #[command(name = "bash")]
        Bash(CompletionOpt),
        /// Generate CLI completions for zsh
        #[command(name = "zsh")]
        Zsh(CompletionOpt),
        /// Generate CLI completions for fish
        #[command(name = "fish")]
        Fish(CompletionOpt),
    }

    impl CompletionCmd {
        pub fn process(self) -> Result<()> {
            let mut app: ClapCommand = RootCmd::command();
            match self {
                Self::Bash(opt) => {
                    generate(Shell::Bash, &mut app, opt.name, &mut std::io::stdout());
                }
                Self::Zsh(opt) => {
                    generate(Shell::Zsh, &mut app, opt.name, &mut std::io::stdout());
                }
                Self::Fish(opt) => {
                    generate(Shell::Fish, &mut app, opt.name, &mut std::io::stdout());
                }
            }
            Ok(())
        }
    }

    /// Search for a Fluvio plugin in the following places:
    ///
    /// - In the directory where the `fluvio` executable is located
    /// - In the system PATH
    /// - In the `~/.fluvio/extensions/` directory
    fn find_plugin(name: &str) -> Option<PathBuf> {
        let ext_dir = fluvio_extensions_dir().ok();
        let self_exe = std::env::current_exe().ok();
        let self_dir = self_exe.as_ref().and_then(|it| it.parent());
        which::which_in(name, self_dir, ".")
            .or_else(|_| which::which(name))
            .or_else(|_| which::which_in(name, ext_dir, "."))
            .ok()
    }

    fn process_external_subcommand(mut args: Vec<String>) -> Result<()> {
        // The external subcommand's name is given as the first argument, take it.
        let cmd = args.remove(0);

        // Check for a matching external command in the environment
        let subcommand = format!("fluvio-{cmd}");
        let subcommand_path = match find_plugin(&subcommand) {
            Some(path) => path,
            None => {
                match fluvio_extensions_dir() {
                    Ok(fluvio_dir) => {
                        println!(
                            "Unable to find plugin '{}'. Make sure it is installed in {:?}.",
                            &subcommand, fluvio_dir,
                        );
                    }
                    Err(_) => {
                        println!(
                            "Unable to find plugin '{}'. Make sure it is in your PATH.",
                            &subcommand,
                        );
                    }
                }
                std::process::exit(1);
            }
        };

        // Print the fully-qualified command to debug
        let args_string = args.join(" ");
        debug!(
            "Launching external subcommand: {} {}",
            subcommand_path.as_path().display(),
            &args_string
        );

        // Execute the command with the provided arguments
        let status = Command::new(subcommand_path.as_path())
            .args(&args)
            .status()?;

        if let Some(code) = status.code() {
            std::process::exit(code);
        }

        #[cfg(unix)]
        {
            // https://doc.rust-lang.org/std/os/unix/process/trait.ExitStatusExt.html
            use std::os::unix::process::ExitStatusExt;
            if let Some(signal) = status.signal() {
                println!("Extension killed via {signal} signal");
                std::process::exit(signal);
            }
        }

        Ok(())
    }

    /// Retrieves `true` if the provided `fluvio` command must trigger an
    /// update check.
    ///
    /// Commands that must trigger update checks are:
    ///
    /// - `fluvio version`
    #[inline]
    fn command_triggers_update_check(cmd: &RootCmd) -> bool {
        matches!(cmd, RootCmd::Version(_))
    }
}

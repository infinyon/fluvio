//! # Root CLI
//!
//! CLI configurations at the top of the tree

mod http;
mod error;
mod install;
mod profile;
pub mod channel;
mod version;
mod metadata;
mod topic;
mod consume;
mod produce;
mod partition;
mod connector;
mod tableformat;
mod smartmodule;
mod derivedstream;
mod render;

use std::env::current_exe;
use crate::channel::{
    FluvioChannelConfig, is_fluvio_bin_in_std_dir, FluvioChannelInfo, ImageTagStrategy,
};

pub(crate) use error::{Result, CliError};

use fluvio_extension_common as common;

pub(crate) const VERSION: &str = include_str!("../../../VERSION");
pub use root::{Root, HelpOpt};

mod root {

    use std::sync::Arc;
    use std::path::PathBuf;
    use std::process::Command;

    use structopt::clap::{AppSettings, Shell, App, SubCommand};
    use structopt::StructOpt;
    use tracing::debug;

    use fluvio::Fluvio;

    #[cfg(feature = "k8s")]
    use fluvio_cluster::cli::ClusterCmd;

    use crate::derivedstream::DerivedStreamCmd;
    use crate::connector::ManagedConnectorCmd;
    use crate::tableformat::TableFormatCmd;
    use crate::topic::TopicCmd;
    use crate::consume::ConsumeOpt;
    use crate::produce::ProduceOpt;
    use crate::partition::PartitionCmd;
    use crate::profile::ProfileOpt;
    use crate::install::update::UpdateOpt;
    use crate::install::plugins::InstallOpt;
    use crate::metadata::{MetadataOpt, subcommand_metadata};
    use crate::version::VersionOpt;
    use crate::install::fluvio_extensions_dir;
    use crate::smartmodule::SmartModuleCmd;
    use crate::common::target::ClusterTarget;
    use crate::common::COMMAND_TEMPLATE;
    use crate::common::PrintTerminal;
    use crate::common::Terminal;

    use super::Result;
    use super::VERSION;

    /// Fluvio Command Line Interface
    #[derive(StructOpt, Debug)]
    pub struct Root {
        #[structopt(flatten)]
        opts: RootOpt,
        #[structopt(subcommand)]
        command: RootCmd,
    }

    impl Root {
        pub async fn process(self) -> Result<()> {
            self.command.process(self.opts).await?;
            Ok(())
        }

        pub fn skip_channel_check(&self) -> bool {
            self.opts.skip_channel_check
        }
    }

    #[derive(StructOpt, Debug)]
    struct RootOpt {
        #[structopt(flatten)]
        target: ClusterTarget,

        #[structopt(long)]
        skip_channel_check: bool,
        // TODO: Include flag for overriding channel choice in config
    }

    impl RootOpt {
        pub fn skip_channel_check(&self) -> bool {
            self.skip_channel_check
        }
    }

    #[derive(Debug, StructOpt)]
    #[structopt(
        about = "Fluvio Command Line Interface",
        name = "fluvio",
        template = COMMAND_TEMPLATE,
        max_term_width = 100,
        global_settings = &[
            AppSettings::VersionlessSubcommands,
            AppSettings::DeriveDisplayOrder,
            AppSettings::DisableVersion,
        ]
    )]
    enum RootCmd {
        /// All top-level commands that require a Fluvio client are bundled in `FluvioCmd`
        #[structopt(flatten)]
        #[cfg(feature = "consumer")]
        Fluvio(FluvioCmd),

        /// Manage Profiles, which describe linked clusters
        ///
        /// Each Profile describes a particular Fluvio cluster you may be connected to.
        /// This might correspond to Fluvio running on Minikube or in the Cloud.
        /// There is one "active" profile, which determines which cluster all of the
        /// Fluvio CLI commands interact with.
        #[structopt(name = "profile")]
        Profile(ProfileOpt),

        /// Install or uninstall Fluvio cluster
        ///
        #[cfg(feature = "k8s")]
        #[structopt(name = "cluster")]
        Cluster(Box<ClusterCmd>),

        /// Install Fluvio plugins
        ///
        /// The Fluvio CLI considers any executable with the prefix `fluvio-` to be a
        /// CLI plugin. For example, an executable named `fluvio-foo` in your PATH may
        /// be invoked by running `fluvio foo`.
        ///
        /// This command allows you to install plugins from Fluvio's package registry.
        #[structopt(name = "install")]
        Install(InstallOpt),

        /// Update the Fluvio CLI
        #[structopt(name = "update")]
        Update(UpdateOpt),

        /// Print Fluvio version information
        #[structopt(name = "version")]
        Version(VersionOpt),

        /// Generate command-line completions for Fluvio
        ///
        /// Run the following two commands to enable fluvio command completions.
        ///
        /// Open a new terminal for the changes to take effect.
        ///
        /// $ fluvio completions bash > ~/fluvio_completions.sh
        /// {n}$ echo "source ~/fluvio_completions.sh" >> ~/.bashrc
        #[structopt(name = "completions")]
        Completions(CompletionCmd),

        /// Generate metadata for Fluvio base CLI
        #[structopt(
            name = "metadata",
            settings = &[AppSettings::Hidden]
        )]
        Metadata(MetadataOpt),

        /// Create and work with Managed Connectors
        #[structopt(name = "connector")]
        ManagedConnector(ManagedConnectorCmd),

        /// Create a TableFormat display specification
        ///
        /// Used with the consumer output type `full_table` to
        /// describe how to render JSON data in a tabular form
        #[structopt(name = "table-format", aliases = &["tf"])]
        TableFormat(TableFormatCmd),

        /// Create and manage DerivedStreams
        ///
        /// Use topics, SmartModules or other DerivedStreams
        /// to build a customized stream to consume
        #[structopt(name = "derived-stream", aliases = &["ds"])]
        DerivedStream(DerivedStreamCmd),

        #[structopt(external_subcommand)]
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
                    let version = semver::Version::parse(crate::VERSION).unwrap();

                    // This is a bit of a hack to contain Channels fully within fluvio-cli

                    // If dev, set the develop flag
                    // If latest, change the image version to <VERSION>-<GIT_HASH>

                    use crate::current_exe;
                    use crate::is_fluvio_bin_in_std_dir;
                    use crate::FluvioChannelConfig;
                    use crate::FluvioChannelInfo;
                    use crate::ImageTagStrategy;

                    // Verify if the current binary is running in the "official" location
                    // If we're not in the fluvio directory then
                    // assume dev mode (i.e., do not exec to other binaries)
                    let current_exe = current_exe()?;

                    // TODO: We need a way to propagate the skip_channel_check
                    if is_fluvio_bin_in_std_dir(&current_exe) && !root.skip_channel_check() {
                        //if is_fluvio_bin_in_std_dir(&current_exe) {
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

                        //println!("Current exe: {:?}", &current_exe);
                        //println!("Config current exe: {:?}", &channel.current_exe());

                        debug!("channel: {:#?}", channel);

                        let current_channel_info = if let Some(channel_info) =
                            channel.get_channel(&channel.current_channel())
                        {
                            channel_info
                        } else {
                            FluvioChannelInfo::stable_channel()
                        };

                        let modified_cluster_cmd = match current_channel_info
                            .get_image_tag_strategy()
                        {
                            ImageTagStrategy::VersionGit => {
                                // If we've specified an image version, use that
                                // Otherwise, use the image version we push to dockerhub
                                let image_version = format!("{}-{}", VERSION, env!("GIT_HASH"));

                                if let ClusterCmd::Start(opts) = *cluster {
                                    if opts.k8_config.image_version.is_none() {
                                        let mut new_start_opts = *opts;
                                        new_start_opts.k8_config.image_version =
                                            Some(image_version);
                                        Box::new(ClusterCmd::Start(Box::new(new_start_opts)))
                                    } else {
                                        Box::new(ClusterCmd::Start(opts))
                                    }
                                } else if let ClusterCmd::Upgrade(opts) = *cluster {
                                    if opts.start.k8_config.image_version.is_none() {
                                        let mut new_upgrade_opts = *opts;
                                        new_upgrade_opts.start.k8_config.image_version =
                                            Some(image_version);
                                        Box::new(ClusterCmd::Upgrade(Box::new(new_upgrade_opts)))
                                    } else {
                                        Box::new(ClusterCmd::Upgrade(opts))
                                    }
                                } else {
                                    cluster
                                }
                            }
                            ImageTagStrategy::Version => cluster,
                            ImageTagStrategy::Git => {
                                // If we are dealing with a cluster start or upgrade, then we care about channels
                                if let ClusterCmd::Start(opts) = *cluster {
                                    let mut new_start_opts = *opts;
                                    new_start_opts.develop = true;
                                    Box::new(ClusterCmd::Start(Box::new(new_start_opts)))
                                } else if let ClusterCmd::Upgrade(opts) = *cluster {
                                    let mut new_upgrade_opts = *opts;
                                    new_upgrade_opts.start.develop = true;
                                    Box::new(ClusterCmd::Upgrade(Box::new(new_upgrade_opts)))
                                } else {
                                    cluster
                                }
                            }
                        };

                        modified_cluster_cmd
                            .process(out, version, root.target)
                            .await?;
                    } else {
                        debug!("Fluvio bin not in standard install location. Assuming dev channel");

                        // If we are dealing with a cluster start or upgrade, then we care about channels
                        let modified_cluster_cmd = if let ClusterCmd::Start(opts) = *cluster.clone()
                        {
                            let mut new_start_opts = *opts;
                            new_start_opts.develop = true;
                            Box::new(ClusterCmd::Start(Box::new(new_start_opts)))
                        } else if let ClusterCmd::Upgrade(opts) = *cluster.clone() {
                            let mut new_upgrade_opts = *opts;
                            new_upgrade_opts.start.develop = true;
                            Box::new(ClusterCmd::Upgrade(Box::new(new_upgrade_opts)))
                        } else {
                            cluster
                        };

                        modified_cluster_cmd
                            .process(out, version, root.target)
                            .await?;
                    }
                }
                Self::Install(install) => {
                    install.process().await?;
                }
                Self::Update(update) => {
                    update.process().await?;
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
                Self::ManagedConnector(group) => {
                    let fluvio = root.target.connect().await?;
                    group.process(out, &fluvio).await?;
                }
                Self::TableFormat(tableformat) => {
                    let fluvio = root.target.connect().await?;
                    tableformat.process(out, &fluvio).await?;
                }
                Self::DerivedStream(derivedstream) => {
                    let fluvio = root.target.connect().await?;
                    derivedstream.process(out, &fluvio).await?;
                }
                Self::External(args) => {
                    process_external_subcommand(args)?;
                }
            }

            Ok(())
        }
    }

    // For some reason this doc string is the one that gets used for the top-level help menu.
    // Please don't change it unless you want to update the top-level help menu "about".
    /// Fluvio command-line interface
    #[derive(StructOpt, Debug)]
    pub enum FluvioCmd {
        /// Read messages from a topic/partition
        #[structopt(name = "consume")]
        Consume(Box<ConsumeOpt>),

        /// Write messages to a topic/partition
        #[structopt(name = "produce")]
        Produce(ProduceOpt),

        /// Manage and view Topics
        ///
        /// A Topic is essentially the name of a stream which carries messages that
        /// are related to each other. Similar to the role of tables in a relational
        /// database, the names and contents of Topics will typically reflect the
        /// structure of the application domain they are used for.
        #[structopt(name = "topic")]
        Topic(TopicCmd),

        /// Manage and view Partitions
        ///
        /// Partitions are a way to divide the total traffic of a single Topic into
        /// separate streams which may be processed independently. Data sent to different
        /// partitions may be processed by separate SPUs on different computers. By
        /// dividing the load of a Topic evenly among partitions, you can increase the
        /// total throughput of the Topic.
        #[structopt(name = "partition")]
        Partition(PartitionCmd),

        /// Create and manage SmartModules
        ///
        /// SmartModules are compiled WASM modules used to create SmartModules.
        #[structopt(name = "smart-module", aliases = &["sm"])]
        SmartModule(SmartModuleCmd),
    }

    impl FluvioCmd {
        /// Connect to Fluvio and pass the Fluvio client to the subcommand handlers.
        pub async fn process<O: Terminal>(self, out: Arc<O>, target: ClusterTarget) -> Result<()> {
            let fluvio_config = target.load()?;
            let fluvio = Fluvio::connect_with_config(&fluvio_config).await?;

            match self {
                Self::Consume(consume) => {
                    consume.process(&fluvio).await?;
                }
                Self::Produce(produce) => {
                    produce.process(&fluvio).await?;
                }
                Self::Topic(topic) => {
                    topic.process(out, &fluvio).await?;
                }
                Self::Partition(partition) => {
                    partition.process(out, &fluvio).await?;
                }
                Self::SmartModule(smartmodule) => {
                    smartmodule.process(out, &fluvio).await?;
                }
            }

            Ok(())
        }
    }

    #[derive(Debug, StructOpt)]
    pub struct HelpOpt {}
    impl HelpOpt {
        pub fn process(self) -> Result<()> {
            let external_commands = subcommand_metadata()?;

            // Add external command definitions to our own clap::App definition
            let mut app: App = Root::clap();
            for i in &external_commands {
                match i.path.file_name() {
                    Some(file_name) => {
                        app = app.subcommand(
                            SubCommand::with_name(
                                file_name.to_string_lossy().strip_prefix("fluvio-").unwrap(),
                            )
                            .about(&*i.meta.description),
                        );
                    }
                    None => {
                        app = app.subcommand(
                            SubCommand::with_name(&*i.meta.title).about(&*i.meta.description),
                        );
                    }
                }
            }

            // Use clap's help printer, loaded up with external subcommands
            let _ = app.print_help();
            Ok(())
        }
    }

    #[derive(Debug, StructOpt)]
    struct CompletionOpt {
        #[structopt(long, default_value = "fluvio")]
        name: String,
    }

    #[derive(Debug, StructOpt)]
    enum CompletionCmd {
        /// Generate CLI completions for bash
        #[structopt(name = "bash")]
        Bash(CompletionOpt),
        // Zsh generation currently has a bug that causes panic
        // /// Generate CLI completions for zsh
        // #[structopt(name = "zsh")]
        // Zsh(CompletionOpt),
        /// Generate CLI completions for fish
        #[structopt(name = "fish")]
        Fish(CompletionOpt),
    }

    impl CompletionCmd {
        pub fn process(self) -> Result<()> {
            let mut app: structopt::clap::App = RootCmd::clap();
            match self {
                Self::Bash(opt) => {
                    app.gen_completions_to(opt.name, Shell::Bash, &mut std::io::stdout());
                }
                // Self::Zsh(opt) => {
                //     app.gen_completions_to(opt.name, Shell::Zsh, &mut std::io::stdout());
                // }
                Self::Fish(opt) => {
                    app.gen_completions_to(opt.name, Shell::Fish, &mut std::io::stdout());
                }
            }
            Ok(())
        }
    }

    /// Search for a Fluvio plugin in the following places:
    ///
    /// - In the system PATH
    /// - In the directory where the `fluvio` executable is located
    /// - In the `~/.fluvio/extensions/` directory
    fn find_plugin(name: &str) -> Option<PathBuf> {
        let ext_dir = fluvio_extensions_dir().ok();
        let self_exe = std::env::current_exe().ok();
        let self_dir = self_exe.as_ref().and_then(|it| it.parent());
        which::which(name)
            .or_else(|_| which::which_in(name, self_dir, "."))
            .or_else(|_| which::which_in(name, ext_dir, "."))
            .ok()
    }

    fn process_external_subcommand(mut args: Vec<String>) -> Result<()> {
        // The external subcommand's name is given as the first argument, take it.
        let cmd = args.remove(0);

        // Check for a matching external command in the environment
        let subcommand = format!("fluvio-{}", cmd);
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
                println!("Extension killed via {} signal", signal);
                std::process::exit(signal);
            }
        }

        Ok(())
    }
}

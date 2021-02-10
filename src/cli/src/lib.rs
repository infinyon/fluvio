//! # Root CLI
//!
//! CLI configurations at the top of the tree

use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::process::Command;
use structopt::clap::{AppSettings, Shell, App, SubCommand};
use structopt::StructOpt;
use tracing::debug;

mod http;
mod error;
mod install;
mod profile;

use profile::ProfileCmd;
use install::update::UpdateOpt;
use install::plugins::InstallOpt;
pub use error::{Result, CliError};

use fluvio::Fluvio;
use fluvio_extension_common::FluvioExtensionMetadata;
use fluvio_extension_consumer::consume::ConsumeLogOpt;
use fluvio_extension_consumer::produce::ProduceLogOpt;
use fluvio_extension_consumer::partition::PartitionCmd;
use fluvio_extension_consumer::topic::TopicCmd;
use fluvio_cluster::cli::ClusterCmd;

use fluvio_extension_common as common;
use common::COMMAND_TEMPLATE;
use common::target::ClusterTarget;
use common::output::Terminal;
use common::PrintTerminal;
use fluvio::config::ConfigFile;

pub const VERSION: &str = include_str!("VERSION");
static_assertions::const_assert!(!VERSION.is_empty());

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
}

#[derive(StructOpt, Debug)]
struct RootOpt {
    #[structopt(flatten)]
    target: ClusterTarget,
}

#[derive(Debug, StructOpt)]
#[structopt(
    about = "Fluvio Command Line Interface",
    name = "fluvio",
    template = COMMAND_TEMPLATE,
    max_term_width = 80,
    global_settings = &[
    AppSettings::VersionlessSubcommands,
    AppSettings::DeriveDisplayOrder,
    AppSettings::DisableVersion,
    ]
)]
enum RootCmd {
    /// All top-level commands that require a Fluvio client are bundled in `FluvioCmd`
    #[structopt(flatten)]
    Fluvio(FluvioCmd),

    /// Manage Profiles, which describe linked clusters
    ///
    /// Each Profile describes a particular Fluvio cluster you may be connected to.
    /// This might correspond to Fluvio running on Minikube or in the Cloud.
    /// There is one "active" profile, which determines which cluster all of the
    /// Fluvio CLI commands interact with.
    #[structopt(name = "profile")]
    Profile(ProfileCmd),

    /// Install or uninstall Fluvio clusters
    ///
    /// If you are not using Fluvio Cloud, you may wish to install your own Fluvio
    /// cluster, running either directly on your computer (local), or hosted inside
    /// of a Minikube kubernetes environment. These cluster commands will help you
    /// to set up these types of installations.
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
    #[structopt(
        name = "completions",
        settings = &[AppSettings::Hidden]
    )]
    Completions(CompletionCmd),

    /// Generate metadata for Fluvio base CLI
    #[structopt(
        name = "metadata",
        settings = &[AppSettings::Hidden]
    )]
    Metadata(MetadataOpt),

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
            Self::Cluster(cluster) => {
                cluster.process(out, crate::VERSION, root.target).await?;
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
    ///
    /// By default, this activates in "follow" mode, where
    /// the command will hang and continue to wait for new
    /// messages, printing them as they arrive. You can use
    /// the `-d` switch to consume all available messages,
    /// but exit upon reaching the end of the stream.
    #[structopt(name = "consume")]
    Consume(ConsumeLogOpt),

    /// Write messages to a topic/partition
    ///
    /// By default, this reads a single line from stdin to
    /// use as the message. If you run this with no file options,
    /// the command will hang until you type a line in the terminal.
    /// Alternatively, you can pipe a message into the command
    /// like this:
    ///
    /// $ echo "Hello, world" | fluvio produce greetings
    #[structopt(name = "produce")]
    Produce(ProduceLogOpt),

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
        }

        Ok(())
    }
}

#[derive(Debug, StructOpt)]
struct VersionOpt {}

impl VersionOpt {
    pub async fn process(self, target: ClusterTarget) -> Result<()> {
        println!("Fluvio CLI        : {}", crate::VERSION.trim());

        // Read CLI and compute its sha256
        let fluvio_bin = std::fs::read(std::env::current_exe()?)?;
        let mut hasher = Sha256::new();
        hasher.update(fluvio_bin);
        let fluvio_bin_sha256 = hasher.finalize();
        println!("Fluvio CLI SHA256 : {:x}", &fluvio_bin_sha256);

        // Attempt to connect to a Fluvio cluster to get platform version
        // Even if we fail to connect, we should not fail the other printouts
        let mut platform_version = String::from("Not available");
        if let Ok(fluvio_config) = target.load() {
            if let Ok(fluvio) = Fluvio::connect_with_config(&fluvio_config).await {
                let version = fluvio.platform_version();
                platform_version = version.to_string();
            }
        }

        let profile_name = ConfigFile::load(None)
            .ok()
            .and_then(|it| {
                it.config()
                    .current_profile_name()
                    .map(|name| name.to_string())
            })
            .map(|name| format!(" ({})", name))
            .unwrap_or_else(|| "".to_string());
        println!("Fluvio Platform   : {}{}", platform_version, profile_name);

        println!("Git Commit        : {}", env!("GIT_HASH"));
        if let Some(os_info) = os_info() {
            println!("OS Details        : {}", os_info);
        }
        println!("Rustc Version     : {}", env!("RUSTC_VERSION"));

        Ok(())
    }
}

/// Fetch OS information
fn os_info() -> Option<String> {
    use sysinfo::SystemExt;
    let sys = sysinfo::System::new_all();

    let info = format!(
        "{} {} (kernel {})",
        sys.get_name()?,
        sys.get_os_version()?,
        sys.get_kernel_version()?,
    );

    Some(info)
}

#[derive(Debug, StructOpt)]
pub struct HelpOpt {}
impl HelpOpt {
    pub fn process(self) -> Result<()> {
        let external_commands = MetadataOpt::metadata(false)?;

        let mut app: App = Root::clap();

        for i in &external_commands {
            app = app.subcommand(SubCommand::with_name(&i.command).about(&*i.description));
        }

        let _ = app.print_help();

        Ok(())
    }
}

#[derive(Debug, StructOpt)]
struct MetadataOpt {}
impl MetadataOpt {
    pub fn process(self) -> Result<()> {
        let metadata = Self::metadata(true)?;
        if let Ok(out) = serde_json::to_string(&metadata) {
            println!("{}", out);
        }

        Ok(())
    }

    pub fn metadata(include_static: bool) -> Result<Vec<FluvioExtensionMetadata>> {
        // Scan for extensions, run `fluvio <extension> metadata` on them, add them to metadata
        // hashmap, run the same on FluvioCmds
        let mut metadata: Vec<FluvioExtensionMetadata> = if include_static {
            vec![
                TopicCmd::metadata(),
                PartitionCmd::metadata(),
                ProduceLogOpt::metadata(),
                ConsumeLogOpt::metadata(),
            ]
        } else {
            Vec::new()
        };

        for (_subcommand_name, subcommand_path) in crate::install::get_extensions()? {
            let stdout = match Command::new(subcommand_path.as_path())
                .args(&["metadata"])
                .output()
            {
                Ok(out) => out.stdout,
                _ => continue,
            };

            if let Ok(out) = serde_json::from_slice::<FluvioExtensionMetadata>(&stdout) {
                metadata.push(out);
            }
        }

        Ok(metadata)
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

fn process_external_subcommand(mut args: Vec<String>) -> Result<()> {
    use std::fs;
    use std::path::PathBuf;

    // The external subcommand's name is given as the first argument, take it.
    let cmd = args.remove(0);
    // Check for a matching external command in the environment

    let external_subcommand = format!("fluvio-{}", cmd);
    let mut subcommand_path: Option<PathBuf> = None;

    let fluvio_dir = crate::install::fluvio_extensions_dir()?;

    if let Ok(entries) = fs::read_dir(&fluvio_dir) {
        for entry in entries {
            if let Ok(entry) = entry {
                if entry.path().ends_with(&external_subcommand) {
                    subcommand_path = Some(entry.path());
                    break;
                }
            }
        }
    }
    let subcommand_path = match subcommand_path {
        Some(path) => path,
        None => {
            println!(
                "Unable to find plugin '{}'. Make sure it is installed in {:?}.",
                &external_subcommand, fluvio_dir,
            );
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

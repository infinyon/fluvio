//!
//! # Root CLI
//!
//! CLI configurations at the top of the tree

use std::sync::Arc;
use structopt::clap::{AppSettings, Shell};
use structopt::StructOpt;
use tracing::debug;

use fluvio_future::task::run_block_on;

use crate::COMMAND_TEMPLATE;
use crate::CliError;

use super::consume::process_consume_log;
use super::produce::process_produce_record;
use super::topic::process_topic;
use super::spu::*;
use super::custom::*;
use super::group::*;
use super::profile::process_profile;
use super::cluster::process_cluster;
use super::consume::ConsumeLogOpt;
use super::produce::ProduceLogOpt;
use super::topic::TopicOpt;
use super::profile::ProfileCommand;
use super::cluster::ClusterCommands;
use super::partition::PartitionOpt;
use crate::install::update::UpdateOpt;
use crate::install::plugins::InstallOpt;

#[cfg(any(feature = "cluster_components", feature = "cluster_components_rustls"))]
use super::run::{process_run, RunOpt};

/// Fluvio Command Line Interface
#[derive(Debug, StructOpt)]
#[structopt(
    about = "Fluvio Command Line Interface",
    name = "fluvio",
    template = COMMAND_TEMPLATE,
    max_term_width = 80,
    global_settings = &[AppSettings::VersionlessSubcommands, AppSettings::DeriveDisplayOrder, AppSettings::DisableVersion]
)]
enum Root {
    /// Reads messages from a topic/partition
    ///
    /// By default, this activates in "follow" mode, where
    /// the command will hang and continue to wait for new
    /// messages, printing them as they arrive. You can use
    /// the `-d` switch to consume all available messages,
    /// but exit upon reaching the end of the stream.
    #[structopt(name = "consume")]
    Consume(ConsumeLogOpt),

    /// Writes messages to a topic/partition
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

    /// Manage and view Streaming Processing Units (SPUs)
    ///
    /// SPUs make up the part of a Fluvio cluster which is in charge
    /// of receiving messages from producers, storing those messages,
    /// and relaying them to consumers. This command lets you see
    /// the status of SPUs in your cluster.
    #[structopt(name = "spu")]
    SPU(SpuOpt),

    /// Manage and view SPU Groups (SPGs)
    ///
    /// SPGs are groups of SPUs in a cluster which are managed together.
    #[structopt(name = "spg")]
    SPUGroup(SpuGroupOpt),

    /// Manage and view "custom SPUs", operated outside a cluster
    ///
    /// A "custom SPU" is just a SPU which exists outside of a typical
    /// Fluvio cluster. As opposed to a regular "managed" SPU, you are
    /// responsible for operating and maintaining custom SPUs.
    ///
    /// This command lets you register and unregister custom SPUs, which tells
    /// the Streaming Controller (SC) of the cluster whether to - and how to -
    /// direct streaming traffic to those SPUs.
    #[structopt(name = "custom-spu")]
    CustomSPU(CustomSpuOpt),

    /// Manage and view Topics
    ///
    /// A Topic is essentially the name of a stream which carries messages that
    /// are related to each other. Similar to the role of tables in a relational
    /// database, the names and contents of Topics will typically reflect the
    /// structure of the application domain they are used for.
    #[structopt(name = "topic")]
    Topic(TopicOpt),

    /// Manage and view Partitions
    ///
    /// Partitions are a way to divide the total traffic of a single Topic into
    /// separate streams which may be processed independently. Data sent to different
    /// partitions may be processed by separate SPUs on different computers. By
    /// dividing the load of a Topic evenly among partitions, you can increase the
    /// total throughput of the Topic.
    #[structopt(name = "partition")]
    Partition(PartitionOpt),

    /// Manage Profiles, which describe linked clusters
    ///
    /// Each Profile describes a particular Fluvio cluster you may be connected to.
    /// This might correspond to Fluvio running on Minikube or in the Cloud.
    /// There is one "active" profile, which determines which cluster all of the
    /// Fluvio CLI commands interact with.
    #[structopt(name = "profile")]
    Profile(ProfileCommand),

    /// Install or uninstall Fluvio clusters
    ///
    /// If you are not using Fluvio Cloud, you may wish to install your own Fluvio
    /// cluster, running either directly on your computer (local), or hosted inside
    /// of a Minikube kubernetes environment. These cluster commands will help you
    /// to set up these types of installations.
    #[structopt(name = "cluster")]
    Cluster(ClusterCommands),

    /// Run a Streaming Controller (SC) or SPU
    #[cfg(any(feature = "cluster_components", feature = "cluster_components_rustls"))]
    #[structopt(name = "run")]
    Run(RunOpt),

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
    Version(VersionCmd),

    /// Generate command-line completions for Fluvio
    #[structopt(
        name = "completions",
        settings = &[AppSettings::Hidden]
    )]
    Completions(CompletionShell),

    #[structopt(external_subcommand)]
    External(Vec<String>),
}

pub fn run_cli(args: &[String]) -> eyre::Result<String> {
    run_block_on(async move {
        let terminal = Arc::new(PrintTerminal::new());

        let root_args: Root = Root::from_iter(args);
        let output = match root_args {
            Root::Consume(consume) => process_consume_log(terminal.clone(), consume).await?,
            Root::Produce(produce) => process_produce_record(terminal.clone(), produce).await?,
            Root::SPU(spu) => process_spu(terminal.clone(), spu).await?,
            Root::SPUGroup(spu_group) => process_spu_group(terminal.clone(), spu_group).await?,
            Root::CustomSPU(custom_spu) => process_custom_spu(terminal.clone(), custom_spu).await?,
            Root::Topic(topic) => process_topic(terminal.clone(), topic).await?,
            Root::Partition(partition) => partition.process_partition(terminal.clone()).await?,
            Root::Profile(profile) => process_profile(terminal.clone(), profile).await?,
            Root::Cluster(cluster) => process_cluster(terminal.clone(), cluster).await?,
            #[cfg(any(feature = "cluster_components", feature = "cluster_components_rustls"))]
            Root::Run(opt) => process_run(opt)?,
            Root::Install(opt) => opt.process().await?,
            Root::Update(opt) => opt.process().await?,
            Root::Version(_) => process_version_cmd()?,
            Root::Completions(shell) => process_completions_cmd(shell)?,
            Root::External(args) => process_external_subcommand(args)?,
        };
        Ok(output)
    })
}

use crate::Terminal;

struct PrintTerminal {}

impl PrintTerminal {
    fn new() -> Self {
        Self {}
    }
}

impl Terminal for PrintTerminal {
    fn print(&self, msg: &str) {
        print!("{}", msg);
    }

    fn println(&self, msg: &str) {
        println!("{}", msg);
    }
}

#[derive(Debug, StructOpt)]
struct VersionCmd {}

fn process_version_cmd() -> Result<String, CliError> {
    println!("Fluvio version : {}", crate::VERSION);
    println!("Git Commit     : {}", env!("GIT_HASH"));
    if let Some(os_info) = option_env!("UNAME") {
        println!("OS Details     : {}", os_info);
    }
    println!("Rustc Version  : {}", env!("RUSTC_VERSION"));
    Ok("".to_owned())
}

#[derive(Debug, StructOpt)]
struct CompletionOpt {
    #[structopt(long, default_value = "fluvio")]
    name: String,
}

#[derive(Debug, StructOpt)]
enum CompletionShell {
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

fn process_completions_cmd(shell: CompletionShell) -> Result<String, CliError> {
    let mut app: structopt::clap::App = Root::clap();
    match shell {
        CompletionShell::Bash(opt) => {
            app.gen_completions_to(opt.name, Shell::Bash, &mut std::io::stdout());
        }
        // CompletionShell::Zsh(opt) => {
        //     app.gen_completions_to(opt.name, Shell::Zsh, &mut std::io::stdout());
        // }
        CompletionShell::Fish(opt) => {
            app.gen_completions_to(opt.name, Shell::Fish, &mut std::io::stdout());
        }
    }
    Ok("".to_string())
}

fn process_external_subcommand(mut args: Vec<String>) -> Result<String, CliError> {
    use std::process::Command;
    use which::{CanonicalPath, Error as WhichError};

    // The external subcommand's name is given as the first argument, take it.
    let cmd = args.remove(0);

    // Check for a matching external command in the environment
    let external_subcommand = format!("fluvio-{}", cmd);
    let subcommand_path = match CanonicalPath::new(&external_subcommand) {
        Ok(path) => path,
        Err(WhichError::CannotFindBinaryPath) => {
            println!(
                "Unable to find plugin '{}'. Make sure it is executable and in your PATH.",
                &external_subcommand
            );
            std::process::exit(1);
        }
        other => other?,
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

    Ok("".to_string())
}

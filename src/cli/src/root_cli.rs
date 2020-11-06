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

#[cfg(any(feature = "cluster_components", feature = "cluster_components_rustls"))]
use super::run::{process_run, RunOpt};

#[derive(Debug, StructOpt)]
#[structopt(
    about = "Fluvio Command Line Interface",
    name = "fluvio",
    template = COMMAND_TEMPLATE,
    global_settings = &[AppSettings::VersionlessSubcommands, AppSettings::DeriveDisplayOrder, AppSettings::DisableVersion]
)]
enum Root {
    #[structopt(
        no_version,
        name = "consume",
        template = COMMAND_TEMPLATE,
        about = "Reads messages from a topic/partition"
    )]
    Consume(ConsumeLogOpt),

    #[structopt(
        name = "produce",
        template = COMMAND_TEMPLATE,
        about = "Writes messages to a topic/partition"
    )]
    Produce(ProduceLogOpt),

    #[structopt(
        name = "spu",
        template = COMMAND_TEMPLATE,
        about = "SPU operations"
    )]
    SPU(SpuOpt),

    #[structopt(
        name = "spu-group",
        template = COMMAND_TEMPLATE,
        about = "SPU group operations"
    )]
    SPUGroup(SpuGroupOpt),

    #[structopt(
        name = "custom-spu",
        template = COMMAND_TEMPLATE,
        about = "Custom SPU operations"
    )]
    CustomSPU(CustomSpuOpt),

    #[structopt(
        name = "topic",
        template = COMMAND_TEMPLATE,
        about = "Topic operations"
    )]
    Topic(TopicOpt),

    #[structopt(
        name = "partition",
        template = COMMAND_TEMPLATE,
        about = "Partition operations"
    )]
    Partition(PartitionOpt),

    #[structopt(
        name = "profile",
        template = COMMAND_TEMPLATE,
        about = "Profile operations"
    )]
    Profile(ProfileCommand),

    #[structopt(
        name = "cluster",
        template = COMMAND_TEMPLATE,
        about = "Cluster operations"
    )]
    Cluster(ClusterCommands),

    #[cfg(any(feature = "cluster_components", feature = "cluster_components_rustls"))]
    #[structopt(about = "Run cluster component")]
    Run(RunOpt),

    #[structopt(
        name = "install",
        template = COMMAND_TEMPLATE,
        about = "Install Fluvio plugins"
    )]
    Install(InstallOpt),

    #[structopt(
        name = "update",
        template = COMMAND_TEMPLATE,
        about = "Update the Fluvio CLI and plugins"
    )]
    Update(UpdateOpt),

    #[structopt(
        name = "version",
        about = "Prints the current fluvio version information"
    )]
    Version(VersionCmd),

    #[structopt(
        name = "completions",
        about = "Generate command-line completions for Fluvio",
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
use crate::install::plugins::InstallOpt;

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

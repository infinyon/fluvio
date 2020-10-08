//!
//! # Root CLI
//!
//! CLI configurations at the top of the tree

use std::sync::Arc;
use structopt::clap::{AppSettings, Shell};
use structopt::StructOpt;

use fluvio::Fluvio;
use fluvio_future::task::run_block_on;

use crate::COMMAND_TEMPLATE;
use crate::CliError;
use crate::target::ClusterTarget;
use crate::Terminal;


use super::consume::process_consume_log;
use super::produce::process_produce_record;
use super::topic::process_topic;
use crate::spu::{SpuOpt, process_spu};
use crate::custom::{CustomSpuOpt, process_custom_spu};
use crate::group::{SpuGroupOpt, process_spu_group};
use super::profile::process_profile;
use super::cluster::process_cluster;
use super::consume::ConsumeLogOpt;
use super::produce::ProduceLogOpt;
use super::topic::TopicOpt;
use super::profile::ProfileCommand;
use super::cluster::ClusterCommands;
use super::partition::PartitionOpt;

#[cfg(feature = "cluster_components")]
use super::run::{process_run, RunOpt};

#[derive(StructOpt, Debug)]
struct Root {
    #[structopt(flatten)]
    target: ClusterTarget,
    #[structopt(subcommand)]
    command: RootCommand,
}

#[derive(Debug, StructOpt)]
#[structopt(
    about = "Fluvio Command Line Interface",
    name = "fluvio",
    template = COMMAND_TEMPLATE,
    global_settings = &[AppSettings::VersionlessSubcommands, AppSettings::DeriveDisplayOrder, AppSettings::DisableVersion]
)]
enum RootCommand {
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

    #[cfg(feature = "cluster_components")]
    #[structopt(about = "Run cluster component")]
    Run(RunOpt),

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
}

pub fn run_cli() -> eyre::Result<String> {
    run_block_on(async move {
        let terminal = Arc::new(PrintTerminal::new());

        let root: Root = Root::from_args();
        let fluvio_config = root.target.load()?;
        let fluvio = Fluvio::connect_with_config(&fluvio_config).await?;

        let output = match root.command {
            RootCommand::Consume(consume) => process_consume_log(terminal.clone(), &fluvio, consume).await?,
            RootCommand::Produce(produce) => process_produce_record(terminal.clone(), &fluvio, produce).await?,
            RootCommand::SPU(spu) => process_spu(terminal.clone(), &fluvio, spu).await?,
            RootCommand::SPUGroup(spu_group) => process_spu_group(terminal.clone(), &fluvio, spu_group).await?,
            RootCommand::CustomSPU(custom_spu) => process_custom_spu(terminal.clone(), &fluvio, custom_spu).await?,
            RootCommand::Topic(topic) => process_topic(terminal.clone(), &fluvio, topic).await?,
            RootCommand::Partition(partition) => partition.process_partition(terminal.clone(), &fluvio).await?,
            RootCommand::Profile(profile) => process_profile(terminal.clone(), profile).await?,
            RootCommand::Cluster(cluster) => process_cluster(terminal.clone(), cluster).await?,
            #[cfg(feature = "cluster_components")]
            RootCommand::Run(opt) => process_run(opt)?,
            RootCommand::Version(_) => process_version_cmd()?,
            RootCommand::Completions(shell) => process_completions_cmd(shell)?,
        };
        Ok(output)
    })
}

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
    let mut app: structopt::clap::App = RootCommand::clap();
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

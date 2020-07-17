//!
//! # Root CLI
//!
//! CLI configurations at the top of the tree

use std::sync::Arc;
use structopt::clap::AppSettings;
use structopt::StructOpt;

use flv_future_aio::task::run_block_on;

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

#[cfg(feature = "cluster_components")]
use super::run::{process_run, RunOpt};

#[derive(Debug, StructOpt)]
#[structopt(
    about = "Fluvio Command Line Interface",
    name = "fluvio",
    template = "{about}

{usage}

{all-args}
",
    global_settings = &[AppSettings::VersionlessSubcommands, AppSettings::DeriveDisplayOrder, AppSettings::DisableVersion]
)]
enum Root {
    #[structopt(
        no_version,
        name = "consume",
        template = "{about}

{usage}

{all-args}
",
        about = "Read messages from a topic/partition"
    )]
    Consume(ConsumeLogOpt),

    #[structopt(
        name = "produce",
        template = "{about}

{usage}

{all-args}
",
        about = "Write messages to a topic/partition"
    )]
    Produce(ProduceLogOpt),

    #[structopt(
        name = "spu",
        template = "{about}

{usage}

{all-args}
",
        about = "SPU Operations"
    )]
    SPU(SpuOpt),

    #[structopt(
        name = "spu-group",
        template = "{about}

{usage}

{all-args}
",
        about = "SPU Group Operations"
    )]
    SPUGroup(SpuGroupOpt),

    #[structopt(
        name = "custom-spu",
        template = "{about}

{usage}

{all-args}
",
        about = "Custom SPU Operations"
    )]
    CustomSPU(CustomSpuOpt),

    #[structopt(
        name = "topic",
        template = "{about}

{usage}

{all-args}
",
        about = "Topic operations"
    )]
    Topic(TopicOpt),


    #[structopt(
        name = "partition",
        template = "{about}

{usage}

{all-args}
",
        about = "Partition operations"
    )]
    Partition(PartitionOpt),


    #[structopt(
        name = "profile",
        template = "{about}

{usage}

{all-args}
",
        about = "Profile operation"
    )]
    Profile(ProfileCommand),

    #[structopt(
        name = "cluster",
        template = "{about}

{usage}

{all-args}
",
        about = "Cluster Operations"
    )]
    Cluster(ClusterCommands),

    #[cfg(feature = "cluster_components")]
    #[structopt(about = "Run cluster component")]
    Run(RunOpt),

    #[structopt(name = "version")]
    Version(VersionCmd),
}

pub fn run_cli() -> Result<String, CliError> {
    run_block_on(async move {
        let terminal = Arc::new(PrintTerminal::new());

        match Root::from_args() {
            Root::Consume(consume) => process_consume_log(terminal.clone(), consume).await,
            Root::Produce(produce) => process_produce_record(terminal.clone(), produce).await,
            Root::SPU(spu) => process_spu(terminal.clone(), spu).await,
            Root::SPUGroup(spu_group) => process_spu_group(terminal.clone(), spu_group).await,
            Root::CustomSPU(custom_spu) => process_custom_spu(terminal.clone(), custom_spu).await,
            Root::Topic(topic) => process_topic(terminal.clone(), topic).await,
            Root::Partition(partition) => partition.process_partition(terminal.clone()).await,
            Root::Profile(profile) => process_profile(terminal.clone(), profile).await,
            Root::Cluster(cluster) => process_cluster(terminal.clone(), cluster).await,
            #[cfg(feature = "cluster_components")]
            Root::Run(opt) => process_run(opt),
            Root::Version(_) => process_version_cmd(),
        }
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
    println!("version is: {}", crate::VERSION);
    Ok("".to_owned())
}

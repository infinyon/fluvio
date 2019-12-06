//!
//! # Root CLI
//!
//! CLI configurations at the top of the tree
//!
use structopt::clap::AppSettings;
use structopt::StructOpt;

use super::consume::process_consume_log;
use super::produce::process_produce_record;
use super::topic::process_topic;
use super::advanced::process_advanced;
use super::spu::all::process_spu;
use super::spu::custom::process_custom_spu;
use super::spu::group::process_spu_group;

use super::consume::ConsumeLogOpt;
use super::produce::ProduceLogOpt;
use super::topic::TopicOpt;
use super::advanced::AdvancedOpt;
use super::spu::all::SpuOpt;
use super::spu::custom::CustomSpuOpt;
use super::spu::group::SpuGroupOpt;

use super::CliError;

#[derive(Debug, StructOpt)]
#[structopt(
    about = "Fluvio Command Line Interface",
    author = "",
    name = "fluvio",
    template = "{about}

{usage}

{all-args}
",
    raw(
        global_settings = "&[AppSettings::VersionlessSubcommands, AppSettings::DeriveDisplayOrder]"
    )
)]
enum Root {
    #[structopt(
        name = "consume",
        author = "",
        template = "{about}

{usage}

{all-args}
", 
        about = "Read messages from a topic/partition"
    )]
    Consume(ConsumeLogOpt),

    #[structopt(
        name = "produce",
        author = "",
        template = "{about}

{usage}

{all-args}
", 
        about = "Write messages to a topic/partition"
    )]
    Produce(ProduceLogOpt),

    #[structopt(name = "spu", author = "", template = "{about}

{usage}

{all-args}
", about = "SPU Operations")]
    SPU(SpuOpt),

    #[structopt(name = "spu-group", author = "", template = "{about}

{usage}

{all-args}
", about = "SPU Group Operations")]
    SPUGroup(SpuGroupOpt),

    #[structopt(name = "custom-spu", author = "", template = "{about}

{usage}

{all-args}
", about = "Custom SPU Operations")]
    CustomSPU(CustomSpuOpt),

    #[structopt(name = "topic", author = "", template = "{about}

{usage}

{all-args}
", about = "Topic operations")]
    Topic(TopicOpt),

    #[structopt(name = "advanced", author = "", template = "{about}

{usage}

{all-args}
", about = "Advanced operations")]
    Advanced(AdvancedOpt),
}

pub fn run_cli() -> Result<(), CliError> {
    match Root::from_args() {
        Root::Consume(consume) => process_consume_log(consume),
        Root::Produce(produce) => process_produce_record(produce),
        Root::SPU(spu) => process_spu(spu),
        Root::SPUGroup(spu_group) => process_spu_group(spu_group),
        Root::CustomSPU(custom_spu) => process_custom_spu(custom_spu),
        Root::Topic(topic) => process_topic(topic),
        Root::Advanced(advanced) => process_advanced(advanced),
    }
}

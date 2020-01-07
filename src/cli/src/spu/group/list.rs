//! # List SPU Groups CLI
//!
//! CLI tree and processing to list SPU Groups
//!

use structopt::StructOpt;

use crate::output::OutputType;
use crate::error::CliError;
use flv_client::profile::ScConfig;
use crate::Terminal;

use super::helpers::list_output::spu_group_response_to_output;

#[derive(Debug)]
pub struct ListSpuGroupsConfig {
    pub output: OutputType,
}



#[derive(Debug, StructOpt)]
pub struct ListManagedSpuGroupsOpt {
    /// Address of Streaming Controller
    #[structopt(short = "c", long = "sc", value_name = "host:port")]
    sc: Option<String>,

    ///Profile name
    #[structopt(short = "P", long = "profile")]
    pub profile: Option<String>,

    /// Output
    #[structopt(
        short = "O",
        long = "output",
        value_name = "type",
        raw(possible_values = "&OutputType::variants()", case_insensitive = "true")
    )]
    output: Option<OutputType>,
}

impl ListManagedSpuGroupsOpt {

    /// Validate cli options and generate config
    fn validate(self) -> Result<(ScConfig, ListSpuGroupsConfig), CliError> {

        let target_server = ScConfig::new(self.sc, self.profile)?;

        // transfer config parameters
        let list_spu_group_cfg = ListSpuGroupsConfig {
            output: self.output.unwrap_or(OutputType::default()),
        };

        // return server separately from topic result
        Ok((target_server, list_spu_group_cfg))
    }

}





/// Process list spus cli request
pub async fn process_list_managed_spu_groups<O: Terminal>(out: std::sync::Arc<O>,opt: ListManagedSpuGroupsOpt) -> Result<(), CliError> {
    
    let (target_server, list_spu_group_cfg) = opt.validate()?;

    let mut sc = target_server.connect().await?;

    let lists = sc.list_group().await?;

    spu_group_response_to_output(out,lists, list_spu_group_cfg.output)

}

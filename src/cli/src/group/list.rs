//! # List SPU Groups CLI
//!
//! CLI tree and processing to list SPU Groups
//!

use structopt::StructOpt;

use flv_client::ClusterConfig;
use flv_metadata::spg::SpuGroupSpec;

use crate::output::OutputType;
use crate::error::CliError;
use crate::Terminal;
use crate::common::OutputFormat;
use crate::target::ClusterTarget;

#[derive(Debug, StructOpt)]
pub struct ListManagedSpuGroupsOpt {
    #[structopt(flatten)]
    output: OutputFormat,

    #[structopt(flatten)]
    target: ClusterTarget,
}

impl ListManagedSpuGroupsOpt {
    /// Validate cli options and generate config
    fn validate(self) -> Result<(ClusterConfig, OutputType), CliError> {
        let target_server = self.target.load()?;

        Ok((target_server, self.output.as_output()))
    }
}

/// Process list spus cli request
pub async fn process_list_managed_spu_groups<O: Terminal>(
    out: std::sync::Arc<O>,
    opt: ListManagedSpuGroupsOpt,
) -> Result<(), CliError> {
    let (target_server, output) = opt.validate()?;

    let mut client = target_server.connect().await?;
    let mut admin = client.admin().await;

    let lists = admin.list::<SpuGroupSpec,_>(vec![]).await?;

    output::spu_group_response_to_output(out, lists, output)
}

mod output {

    //!
    //! # Fluvio SC - output processing
    //!
    //! Format SPU Group response based on output type

    use prettytable::Row;
    use prettytable::row;
    use prettytable::Cell;
    use prettytable::cell;
    use prettytable::format::Alignment;
    use log::debug;

    use flv_client::metadata::objects::Metadata;
    use flv_metadata::spg::SpuGroupSpec;

    use crate::error::CliError;
    use crate::output::OutputType;
    use crate::TableOutputHandler;
    use crate::Terminal;
    use crate::t_println;

    type ListSpuGroups = Vec<Metadata<SpuGroupSpec>>;

    // -----------------------------------
    // Format Output
    // -----------------------------------

    /// Format SPU Group based on output type
    pub fn spu_group_response_to_output<O: Terminal>(
        out: std::sync::Arc<O>,
        list_spu_groups: ListSpuGroups,
        output_type: OutputType,
    ) -> Result<(), CliError> {
        debug!("groups: {:#?}", list_spu_groups);

        if list_spu_groups.len() > 0 {
            out.render_list(&list_spu_groups, output_type)
        } else {
            t_println!(out, "no groups");
            Ok(())
        }
    }

    // -----------------------------------
    // Output Handlers
    // -----------------------------------
    impl TableOutputHandler for ListSpuGroups {
        /// table header implementation
        fn header(&self) -> Row {
            row!["NAME", "REPLICAS", "MIN ID", "RACK", "SIZE", "STATUS",]
        }

        /// return errors in string format
        fn errors(&self) -> Vec<String> {
            self.iter().map(|_g| "".to_owned()).collect()
        }

        /// table content implementation
        fn content(&self) -> Vec<Row> {
            self.iter()
                .map(|r| {
                    let spec = &r.spec;
                    let storage_config = spec.spu_config.real_storage_config();
                    Row::new(vec![
                        Cell::new_align(&r.name, Alignment::RIGHT),
                        Cell::new_align(&spec.replicas.to_string(), Alignment::CENTER),
                        Cell::new_align(&r.spec.min_id.to_string(), Alignment::RIGHT),
                        Cell::new_align(
                            &spec.spu_config.rack.clone().unwrap_or_default(),
                            Alignment::RIGHT,
                        ),
                        Cell::new_align(&storage_config.size, Alignment::RIGHT),
                        Cell::new_align(&r.status.to_string(), Alignment::RIGHT),
                    ])
                })
                .collect()
        }
    }
}

mod list;
pub use cli::*;

mod cli {

    use structopt::StructOpt;

    use crate::COMMAND_TEMPLATE;
    use crate::Terminal;
    use crate::CliError;

    use super::list::ListPartitionOpt;

    #[derive(Debug, StructOpt)]
    pub enum PartitionOpt {
        /// List all of the Partitions in this cluster
        #[structopt(
            name = "list",
            template = COMMAND_TEMPLATE,
        )]
        List(ListPartitionOpt),
    }

    impl PartitionOpt {
        pub(crate) async fn process_partition<O>(
            self,
            out: std::sync::Arc<O>,
        ) -> Result<String, CliError>
        where
            O: Terminal,
        {
            match self {
                Self::List(list) => list.process(out).await,
            }
        }
    }
}

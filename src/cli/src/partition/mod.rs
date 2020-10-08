mod list;
pub use cli::*;

mod cli {

    use structopt::StructOpt;

    use crate::COMMAND_TEMPLATE;
    use crate::Terminal;
    use crate::CliError;

    use super::list::ListPartitionOpt;
    use fluvio::Fluvio;

    #[derive(Debug, StructOpt)]
    #[structopt(name = "partition", about = "Partition operations")]
    pub enum PartitionOpt {
        #[structopt(
            name = "list",
            template = COMMAND_TEMPLATE,
            about = "Show all partitions"
        )]
        List(ListPartitionOpt),
    }

    impl PartitionOpt {
        pub(crate) async fn process_partition<O>(
            self,
            out: std::sync::Arc<O>,
            fluvio: &Fluvio,
        ) -> Result<String, CliError>
        where
            O: Terminal,
        {
            match self {
                Self::List(list) => list.process(out, fluvio).await,
            }
        }
    }
}

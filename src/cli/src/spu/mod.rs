mod list;
mod display;

pub use display::*;
pub use cli::*;

mod cli {

    use super::*;
    use list::ListSpusOpt;
    use list::process_list_spus;

    use structopt::StructOpt;

    use crate::error::CliError;
    use crate::Terminal;

    #[derive(Debug, StructOpt)]
    pub enum SpuOpt {
        #[structopt(
            name = "list",
            template = "{about}

    {usage}

    {all-args}
    ",
            about = "List custom & managed SPUs"
        )]
        List(ListSpusOpt),
    }

    pub(crate) async fn process_spu<O>(
        out: std::sync::Arc<O>,
        spu_opt: SpuOpt,
    ) -> Result<String, CliError>
    where
        O: Terminal,
    {
        (match spu_opt {
            SpuOpt::List(spu_opt) => process_list_spus(out, spu_opt).await,
        })
        .map(|_| format!(""))
    }
}
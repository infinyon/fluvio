mod list;

use list::ListSpusOpt;
use list::process_list_spus;

use structopt::StructOpt;

use crate::error::CliError;

#[derive(Debug, StructOpt)]
pub enum SpuOpt {
    #[structopt(name = "list", author = "", template = "{about}

{usage}

{all-args}
", about = "List custom & managed SPUs")]
    List(ListSpusOpt),
}

pub(crate) fn process_spu(spu_opt: SpuOpt) -> Result<(), CliError> {
    match spu_opt {
        SpuOpt::List(spu_opt) => process_list_spus(spu_opt),
    }
}

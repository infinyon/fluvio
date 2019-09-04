mod create;
mod list;
mod delete;
mod helpers;

use structopt::StructOpt;

use create::CreateCustomSpuOpt;
use create::process_create_custom_spu;

use delete::DeleteCustomSpuOpt;
use delete::process_delete_custom_spu;

use list::ListCustomSpusOpt;
use list::process_list_custom_spus;

use crate::error::CliError;

#[derive(Debug, StructOpt)]
pub enum CustomSpuOpt {
    #[structopt(name = "create", author = "", template = "{about}

{usage}

{all-args}
", about = "Create custom SPU")]
    Create(CreateCustomSpuOpt),

    #[structopt(name = "delete", author = "", template = "{about}

{usage}

{all-args}
", about = "Delete custom SPU")]
    Delete(DeleteCustomSpuOpt),

    #[structopt(name = "list", author = "", template = "{about}

{usage}

{all-args}
", about = "List custom SPUs")]
    List(ListCustomSpusOpt),
}

pub(crate) fn process_custom_spu(custom_spu_opt: CustomSpuOpt) -> Result<(), CliError> {
    match custom_spu_opt {
        CustomSpuOpt::Create(custom_spu_opt) => process_create_custom_spu(custom_spu_opt),
        CustomSpuOpt::Delete(custom_spu_opt) => process_delete_custom_spu(custom_spu_opt),
        CustomSpuOpt::List(custom_spu_opt) => process_list_custom_spus(custom_spu_opt),
    }
}

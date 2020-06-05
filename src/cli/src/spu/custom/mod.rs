mod register;
mod list;
mod unregister;

use structopt::StructOpt;

use register::RegisterCustomSpuOpt;
use register::process_register_custom_spu;

use unregister::UnregisterCustomSpuOpt;
use unregister::process_unregister_custom_spu;

use list::ListCustomSpusOpt;
use list::process_list_custom_spus;

use crate::error::CliError;
use crate::Terminal;

#[derive(Debug, StructOpt)]
pub enum CustomSpuOpt {
    #[structopt(
        name = "register",
        template = "{about}

{usage}

{all-args}
",
        about = "Create custom SPU"
    )]
    Create(RegisterCustomSpuOpt),

    #[structopt(
        name = "unregister",
        template = "{about}

{usage}

{all-args}
",
        about = "Delete custom SPU"
    )]
    Delete(UnregisterCustomSpuOpt),

    #[structopt(
        name = "list",
        template = "{about}

{usage}

{all-args}
",
        about = "List custom SPUs"
    )]
    List(ListCustomSpusOpt),
}

pub(crate) async fn process_custom_spu<O: Terminal>(
    out: std::sync::Arc<O>,
    custom_spu_opt: CustomSpuOpt,
) -> Result<String, CliError> {
    (match custom_spu_opt {
        CustomSpuOpt::Create(custom_spu_opt) => process_register_custom_spu(custom_spu_opt).await,
        CustomSpuOpt::Delete(custom_spu_opt) => process_unregister_custom_spu(custom_spu_opt).await,
        CustomSpuOpt::List(custom_spu_opt) => process_list_custom_spus(out, custom_spu_opt).await,
    })
    .map(|_| format!(""))
}

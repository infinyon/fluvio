mod create;
mod delete;
mod list;

pub use cli::*;
pub use create::*;

mod cli {
    use structopt::StructOpt;

    use super::*;

    use create::CreateManagedSpuGroupOpt;
    use create::process_create_managed_spu_group;

    use delete::DeleteManagedSpuGroupOpt;
    use delete::process_delete_managed_spu_group;

    use list::ListManagedSpuGroupsOpt;
    use list::process_list_managed_spu_groups;

    use crate::COMMAND_TEMPLATE;
    use crate::error::CliError;
    use crate::Terminal;

    #[derive(Debug, StructOpt)]
    pub enum SpuGroupOpt {
        #[structopt(
            name = "create",
            template = COMMAND_TEMPLATE,
            about = "Create managed SPU group"
        )]
        Create(CreateManagedSpuGroupOpt),

        #[structopt(
            name = "delete",
            template = COMMAND_TEMPLATE,
            about = "Delete managed SPU group"
        )]
        Delete(DeleteManagedSpuGroupOpt),

        #[structopt(
            name = "list",
            template = COMMAND_TEMPLATE,
            about = "List managed SPU groups"
        )]
        List(ListManagedSpuGroupsOpt),
    }

    pub(crate) async fn process_spu_group<O: Terminal>(
        out: std::sync::Arc<O>,
        spu_group_opt: SpuGroupOpt,
    ) -> Result<String, CliError> {
        match spu_group_opt {
            SpuGroupOpt::Create(spu_group_opt) => {
                process_create_managed_spu_group(spu_group_opt).await?;
            }
            SpuGroupOpt::Delete(spu_group_opt) => {
                process_delete_managed_spu_group(spu_group_opt).await?;
            }
            SpuGroupOpt::List(spu_group_opt) => {
                process_list_managed_spu_groups(out, spu_group_opt).await?;
            }
        }
        Ok("".to_string())
    }
}

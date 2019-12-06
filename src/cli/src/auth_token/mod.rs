mod create;
mod list;
mod delete;
mod query_metadata;

use structopt::StructOpt;

use create::CreateAuthTokenOpt;
use create::process_create_auth_token;
use list::ListAuthTokensOpt;
use list::process_list_auth_tokens;

use delete::DeleteAuthTokenOpt;
use delete::process_delete_auth_token;

use super::CliError;

#[derive(Debug, StructOpt)]
pub enum AuthTokenOpt {
    #[structopt(name = "create", author = "", about = "Create auth token")]
    Create(CreateAuthTokenOpt),
    #[structopt(name = "list", author = "", about = "List auth tokens")]
    List(ListAuthTokensOpt),
    #[structopt(name = "delete", author = "", about = "Delete an auth token")]
    Delete(DeleteAuthTokenOpt),
}

pub(crate) fn process_auth_tokens(auth_token_opt: AuthTokenOpt) -> Result<(), CliError> {
    match auth_token_opt {
        AuthTokenOpt::Create(create_token_opt) => process_create_auth_token(create_token_opt),
        AuthTokenOpt::List(list_token_opt) => process_list_auth_tokens(list_token_opt),
        AuthTokenOpt::Delete(delete_token_opt) => process_delete_auth_token(delete_token_opt),
    }
}

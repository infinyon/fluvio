use structopt::StructOpt;

use crate::CliError;

pub use context::process_minikube_context;

#[derive(Debug, StructOpt)]
pub struct SetMinikubeContext {
    /// set context name
    #[structopt(long, value_name = "name")]
    pub name: Option<String>,
}

mod context {

    use super::*;

    /// Performs following
    ///     add  IP address to /etc/host
    ///     create new kubectl cluster and context which uses minikube name
    pub fn process_minikube_context(ctx: SetMinikubeContext) -> Result<String, CliError> {
        use k8_config::context::Option;
        use k8_config::context::create_dns_context;

        let mut option = Option::default();
        if let Some(name) = ctx.name {
            option.ctx_name = name;
        }

        create_dns_context(option);

        Ok("".to_owned())
    }
}

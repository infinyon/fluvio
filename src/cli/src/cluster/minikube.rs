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
    use k8_config::context::MinikubeContext;

    /// Performs following
    ///     add  IP address to /etc/host
    ///     create new kubectl cluster and context which uses minikube name
    pub fn process_minikube_context(ctx: SetMinikubeContext) -> Result<String, CliError> {
        let mut context = MinikubeContext::try_from_system()?;

        if let Some(name) = ctx.name {
            context = context.with_name(name);
        }
        context.save()?;

        Ok("".to_owned())
    }
}

use structopt::StructOpt;
use crate::CliError;
use fluvio_index::{PackageId, HttpAgent};

const FLUVIO_PACKAGE_ID: &str = "fluvio/fluvio";

#[derive(StructOpt, Debug)]
pub struct UpdateOpt {
    /// Self-update the Fluvio CLI
    #[structopt(long = "self")]
    update_me: bool,
    /// Update the named package
    package: Option<PackageId>,
}

impl UpdateOpt {
    pub async fn process(self) -> Result<String, CliError> {
        let agent = HttpAgent::new();
        let index_request = agent.request_index()?;
        let index_response = crate::http::execute(index_request).await?;
        let index = agent.index_from_response(index_response).await?;
        println!("Got index: {:#?}", &index);

        if self.update_me {
            update_self(&agent).await?;
            return Ok("".to_string());
        }

        if let Some(id) = self.package {
            update_package(id)?;
            return Ok("".to_string())
        }

        update_all()?;
        Ok("".to_string())
    }
}

async fn update_self(agent: &HttpAgent) -> Result<String, CliError> {
    let id: PackageId = FLUVIO_PACKAGE_ID.parse::<PackageId>()?;
    let request = agent.request_package(&id)?;
    let response = crate::http::execute(request).await?;
    let package = agent.package_from_response(response).await?;
    let latest_release = package.latest_release()?;

    todo!()
}

fn update_package(id: PackageId) -> Result<String, CliError> {
    todo!()
}

fn update_all() -> Result<String, CliError> {
    todo!()
}

use crate::CliError;
use super::*;
use fluvio_cluster::HelmClient;

const DEFAULT_CHART_NAME: &str = "fluvio/fluvio-app";

#[derive(Debug, StructOpt)]
pub enum ReleasesCommand {
    /// show list of versions
    #[structopt(name = "list")]
    List,
}

pub fn process_releases(opt: ReleasesCommand) -> Result<String, CliError> {
    match opt {
        ReleasesCommand::List => list_releases()?,
    }
    Ok("".to_string())
}

fn list_releases() -> Result<(), CliError> {
    let helm_client = HelmClient::new()?;
    let versions = helm_client.versions(DEFAULT_CHART_NAME)?;
    if !versions.is_empty() {
        println!("VERSION");
        for chart in &versions {
            println!("{}", chart.version());
        }
    } else {
        println!("No releases found");
    }
    Ok(())
}

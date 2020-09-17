use crate::CliError;
use super::*;
use fluvio_cluster::ClusterInstaller;

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
    let versions = ClusterInstaller::versions()?;
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

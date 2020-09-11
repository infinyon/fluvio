use crate::CliError;
use super::*;
use fluvio_cluster::list_releases;

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

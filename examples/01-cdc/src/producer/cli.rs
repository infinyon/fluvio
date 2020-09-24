use std::path::PathBuf;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "cdc-producer", about = "CDC producer for mysql server")]
pub struct CliOpt {
    /// Producer profile file
    #[structopt(value_name = "producer-profile.toml", parse(from_os_str))]
    pub profile: PathBuf,
}

pub fn get_cli_opt() -> CliOpt {
    CliOpt::from_args()
}

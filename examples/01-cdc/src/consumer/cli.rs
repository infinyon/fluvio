use std::path::PathBuf;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "cdc-consumer", about = "CDC consumer for mysql server")]
pub struct CliOpt {
    /// Consumer profile file
    #[structopt(value_name = "consumer-profile.toml", parse(from_os_str))]
    pub profile: PathBuf,
}

pub fn get_cli_opt() -> CliOpt {
    CliOpt::from_args()
}

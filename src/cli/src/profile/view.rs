use structopt::StructOpt;

use fluvio::config::ConfigFile;
use crate::Result;

#[derive(Debug, StructOpt)]
pub struct ViewOpt {}

impl ViewOpt {
    pub async fn process(self) -> Result<()> {
        let config_file = ConfigFile::load(None)?;
        println!("{:#?}", config_file.config());
        Ok(())
    }
}

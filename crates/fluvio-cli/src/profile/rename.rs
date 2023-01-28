use clap::Parser;
use anyhow::Result;

use fluvio::config::ConfigFile;

#[derive(Parser, Debug)]
pub struct RenameOpt {
    /// The name of the profile to rename
    pub from: String,
    /// The new name to give the profile
    pub to: String,
}

impl RenameOpt {
    pub fn process(self) -> Result<()> {
        let mut config_file = match ConfigFile::load(None) {
            Ok(config) => config,
            Err(e) => {
                eprintln!("unable to find Fluvio config file");
                return Err(e.into());
            }
        };

        config_file.mut_config().rename_profile(&self.from, self.to);
        config_file.save()?;
        Ok(())
    }
}

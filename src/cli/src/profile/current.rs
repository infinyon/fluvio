use structopt::StructOpt;
use fluvio::config::ConfigFile;
use crate::Result;

#[derive(Debug, StructOpt)]
pub struct CurrentOpt {}

impl CurrentOpt {
    pub async fn process(self) -> Result<()> {
        match ConfigFile::load(None) {
            Ok(config_file) => {
                if let Some(profile) = config_file.config().current_profile_name() {
                    println!("{}", profile);
                } else {
                    println!("no current profile set");
                }
            }
            Err(_) => println!("no profile can be founded"),
        }

        Ok(())
    }
}

pub mod channel;
use channel::ChannelOpt;
use crate::Result;

use structopt::StructOpt;

#[derive(Debug, StructOpt)]
pub struct ConfigOpt {
    #[structopt(subcommand)]
    cmd: ConfigCmd,
}

impl ConfigOpt {
    pub async fn process(self) -> Result<()> {
        self.cmd.process().await?;
        Ok(())
    }
}

#[derive(Debug, StructOpt)]
#[structopt(about = "Available Commands")]
pub enum ConfigCmd {
    /// CLI release channel (Experimental)
    #[structopt(name = "channel")]
    Channel(ChannelOpt),
}

impl ConfigCmd {
    pub async fn process(self) -> Result<()> {
        match self {
            ConfigCmd::Channel(channel) => channel.process().await?,
        }
        Ok(())
    }
}

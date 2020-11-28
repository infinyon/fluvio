use structopt::StructOpt;
use crate::ClusterInstaller;
use crate::extension::Result;

#[derive(Debug, StructOpt)]
pub enum ReleasesCmd {
    /// Show a list of Fluvio release versions
    #[structopt(name = "list")]
    List(ListOpt),
}

impl ReleasesCmd {
    pub async fn process(self) -> Result<()> {
        match self {
            Self::List(list) => {
                list.process().await?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, StructOpt)]
pub struct ListOpt {}

impl ListOpt {
    pub async fn process(self) -> Result<()> {
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
}

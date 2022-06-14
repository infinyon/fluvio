use std::sync::Arc;
use clap::Parser;
use crate::Result;
use crate::common::output::Terminal;
use fluvio::Fluvio;

mod create;
mod list;
mod delete;
mod generate;
mod test;

use self::create::CreateSmartModuleOpt;
use self::list::ListSmartModuleOpt;
use self::delete::DeleteSmartModuleOpt;
use self::generate::GenerateSmartModuleOpt;
use self::test::TestSmartModuleOpt;
use self::local_modules::*;

#[derive(Debug, Parser)]
pub enum SmartModuleCmd {
    Create(CreateSmartModuleOpt),
    List(ListSmartModuleOpt),
    Delete(DeleteSmartModuleOpt),
    Generate(GenerateSmartModuleOpt),
    Test(TestSmartModuleOpt),
}

impl SmartModuleCmd {
    pub async fn process<O: Terminal>(self, out: Arc<O>, fluvio: &Fluvio) -> Result<()> {
        match self {
            Self::Create(opt) => {
                opt.process(fluvio).await?;
            }
            Self::List(opt) => {
                opt.process(out, fluvio).await?;
            }
            Self::Delete(opt) => {
                opt.process(fluvio).await?;
            }
            Self::Generate(opt) => {
                opt.process(fluvio).await?;
            }
            Self::Test(opt) => {
                opt.process(fluvio).await?;
            }
        }
        Ok(())
    }
}

mod local_modules {

    use std::path::PathBuf;

    use tracing::info;

    use fluvio_cli_common::install::fluvio_base_dir;

    use crate::Result;

    pub fn fluvio_smart_dir() -> Result<PathBuf> {
        let base_dir = fluvio_base_dir()?;
        let path = base_dir.join("sm");
        if !path.exists() {
            info!(log = ?path,"creating smart module directory");
            std::fs::create_dir(&path)?;
        }
        Ok(path)
    }
}

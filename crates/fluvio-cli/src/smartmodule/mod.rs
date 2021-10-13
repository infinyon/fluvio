use structopt::StructOpt;
use crate::Result;

mod create;
mod list;
mod describe;
mod delete;

use self::create::CreateSmartModuleOpt;
use self::list::ListSmartModuleOpt;
use self::describe::DescribeSmartModuleOpt;
use self::delete::DeleteSmartModuleOpt;

#[derive(Debug, StructOpt)]
pub enum SmartModuleCmd {
    Create(CreateSmartModuleOpt),
    List(ListSmartModuleOpt),
    Describe(DescribeSmartModuleOpt),
    Delete(DeleteSmartModuleOpt),
}

impl SmartModuleCmd {
    pub fn process(self) -> Result<()> {
        match self {
            Self::Create(opt) => {
                opt.process()?;
            }
            Self::List(opt) => {
                opt.process()?;
            }
            Self::Describe(opt) => {
                opt.process()?;
            }
            Self::Delete(opt) => {
                opt.process()?;
            }
        }
        Ok(())
    }
}

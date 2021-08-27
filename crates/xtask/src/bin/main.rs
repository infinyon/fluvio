use structopt::StructOpt;
use color_eyre::Result;

fn main() -> Result<()> {
    let root: Root = Root::from_args();
    root.cmd.process()?;
    Ok(())
}

#[derive(StructOpt, Debug)]
struct Root {
    #[structopt(subcommand)]
    cmd: RootCmd,
}

#[derive(StructOpt, Debug)]
enum RootCmd {
    Build,
    InstallTarget(InstallTargetOpt),
}

impl RootCmd {
    fn process(self) -> Result<()> {
        match self {
            Self::Build => {
                xtask::build()?;
            }
            Self::InstallTarget(opt) => {
                xtask::install_target(opt.target.as_deref())?;
            }
        }
        Ok(())
    }
}

#[derive(StructOpt, Debug)]
struct InstallTargetOpt {
    target: Option<String>,
}

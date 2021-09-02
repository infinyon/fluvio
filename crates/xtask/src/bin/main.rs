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
    Clippy,
    Test,
    #[structopt(aliases = &["test-unit", "unit-test", "unit-tests"])]
    TestUnits,
    #[structopt(aliases = &["test-doc", "doc-test", "doc-tests"])]
    TestDocs,
    TestClientDocs,
    #[structopt(aliases = &["integration-test", "integration-tests"])]
    TestIntegration,
    InstallTarget(InstallTargetOpt),
}

impl RootCmd {
    fn process(self) -> Result<()> {
        match self {
            Self::Build => {
                xtask::build()?;
            }
            Self::Clippy => {
                xtask::clippy()?;
            }
            Self::Test => {
                xtask::test()?;
            }
            Self::TestDocs => {
                xtask::test_docs()?;
            }
            Self::TestClientDocs => {
                xtask::test_client_docs()?;
            }
            Self::TestUnits => {
                xtask::test_units()?;
            }
            Self::TestIntegration => {
                xtask::test_integration()?;
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

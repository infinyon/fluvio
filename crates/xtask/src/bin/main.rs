use structopt::StructOpt;
use color_eyre::Result;
use xtask::Root;

fn main() -> Result<()> {
    let root: Root = Root::from_args();
    root.cmd.process()?;
    Ok(())
}

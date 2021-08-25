use structopt::StructOpt;
use color_eyre::{Result, eyre::eyre};

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
    Test,
}

impl RootCmd {
    fn process(self) -> Result<()> {
        println!("Cargo is {}", env!("CARGO"));
        Ok(())
    }
}

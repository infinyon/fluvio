mod cmd;
mod generate;
mod build;
mod test;
mod deploy;
mod publish;
mod set_public;

fn main() -> anyhow::Result<()> {
    use clap::Parser;

    use cmd::CdkCommand;

    fluvio_future::subscriber::init_tracer(None);

    let root = CdkCommand::parse();
    root.process()?;

    Ok(())
}

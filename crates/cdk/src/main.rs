mod cmd;
mod build;
mod deploy;

fn main() -> anyhow::Result<()> {
    use clap::Parser;

    use cmd::CdkCommand;

    fluvio_future::subscriber::init_tracer(None);

    let root = CdkCommand::parse();
    root.process()?;

    Ok(())
}

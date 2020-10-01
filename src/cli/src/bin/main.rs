use color_eyre::eyre::Result;

fn main() -> Result<()> {
    fluvio_future::subscriber::init_tracer(None);
    color_eyre::install()?;

    let output = fluvio_cli::run_cli()?;
    if !output.is_empty() {
        println!("{}", output)
    }
    Ok(())
}

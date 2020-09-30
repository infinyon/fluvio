fn main() -> anyhow::Result<()> {
    fluvio_future::subscriber::init_tracer(None);

    let output = fluvio_cli::run_cli()?;
    if !output.is_empty() {
        println!("{}", output)
    }
    Ok(())
}

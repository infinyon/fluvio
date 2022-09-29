
use color_eyre::eyre::Result;

fn main() -> Result<()> {
    fluvio_future::subscriber::init_tracer(None);



    Ok(())
}

pub(crate) mod root;
pub(crate) mod fluvio;
pub(crate) mod util;

fn main() {
    use root::RootCommand;
    use clap::Parser;

    fluvio_future::subscriber::init_tracer(None);

    let root = RootCommand::parse();

    if let Err(e) = fluvio_future::task::run_block_on(root.process()) {
        eprintln!("{e:?}");
        std::process::exit(1);
    }
}

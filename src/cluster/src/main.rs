use structopt::StructOpt;
use fluvio_cluster::cli::ClusterOpt;
use fluvio_future::task::run_block_on;

fn main() -> color_eyre::Result<()> {
    color_eyre::config::HookBuilder::blank()
        .display_env_section(false)
        .install()?;
    let root: ClusterOpt = ClusterOpt::from_args();
    run_block_on(root.process())?;
    Ok(())
}

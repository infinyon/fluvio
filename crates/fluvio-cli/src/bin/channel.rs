//use structopt::StructOpt;
use color_eyre::eyre::Result;
use tracing::{debug};

// Create custom channels
// Support Version number release channels

// If possible, accept no args, or find a way to squash in the Fluvio ones that we'll forward
// would like if `fluvio --help` was handled by the channel binary

// OR act as a Fluvio frontend only if this binary is named `fluvio`
fn main() -> Result<()> {
    fluvio_future::subscriber::init_tracer(None);

    debug!("Check if running as fluvio frontend");

    // Check on the name this binary was called. If it is `fluvio` be in transparent frontend-mode
    debug!("Binary is named `fluvio` - Frontend mode");

    // If development-mode, use the `--help` output from `fluvio-channel`
    debug!("Binary is not named `fluvio` - Development mode");
    // Read in args
    // Handle what is `fluvio-channel` specific

    // //

    // Look for channel config

    // If it doesn't exist do nothing, default channel is Stable

    // If it does exist, look up the current channel
    // Load the the Channel Info

    // Fluvio Binary: ~/.fluvio/bin/fluvio (default)
    // Extensions directory: ~/.fluvio/extensions (default)
    // K8 image: infinyon/fluvio (default)
    // Image tag format: {version (default), version-git, git}

    // Fluvio binary resolution order:
    // From channel config
    // In PATH
    // In default Fluvio directory ($HOME/.fluvio/bin)
    // In current directory
    // From FLUVIO_BIN env var

    Ok(())
}

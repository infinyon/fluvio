pub mod update;
pub mod plugins;

fn error_convert(err: fluvio_cli_common::error::CliError) -> crate::error::CliError {
    crate::error::CliError::FluvioInstall(err)
}

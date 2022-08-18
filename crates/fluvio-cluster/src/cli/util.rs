use std::io::Error as IoError;
use std::io::ErrorKind;
use std::process::Command;

use tracing::debug;

/// get path to the binary
#[allow(unused)]
pub fn get_binary(bin_name: &str) -> Result<Command, IoError> {
    let current_exe =
        std::env::current_exe().expect("Failed to get the path of the integration test binary");
    let mut bin_dir = current_exe
        .parent()
        .expect("failed to get parent")
        .to_owned();
    bin_dir.push(bin_name);
    bin_dir.set_extension(std::env::consts::EXE_EXTENSION);

    debug!("try to get binary: {:#?}", bin_dir);
    if !bin_dir.exists() {
        Err(IoError::new(
            ErrorKind::NotFound,
            format!("{} not found in: {:#?}", bin_name, bin_dir),
        ))
    } else {
        Ok(Command::new(bin_dir.into_os_string()))
    }
}

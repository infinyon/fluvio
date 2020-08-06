use std::io::Error as IoError;
use std::io::ErrorKind;
use std::process::Command;

use log::debug;

pub fn get_fluvio() -> Result<Command, IoError> {
    get_binary("fluvio")
}

/// get path to the binary
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
            format!("{} not founded in: {:#?}", bin_name, bin_dir),
        ))
    } else {
        Ok(Command::new(bin_dir.into_os_string()))
    }
}

use std::fs::File;
use std::process::Stdio;

#[allow(unused)]
pub fn open_log(prefix: &str) -> (File, File) {
    let output = File::create(format!("/tmp/flv_{}.out", prefix)).expect("log file");
    let error = File::create(format!("/tmp/flv_{}.log", prefix)).expect("err file");

    (output, error)
}

#[allow(unused)]
pub fn command_exec(binary: &str, prefix: &str, process: impl Fn(&mut Command)) {
    let mut cmd = get_binary(binary).unwrap_or_else(|_| panic!("unable to get binary: {}", binary));

    let (output, error) = open_log(prefix);

    cmd.stdout(Stdio::from(output)).stderr(Stdio::from(error));

    process(&mut cmd);

    let status = cmd.status().expect("topic creation failed");

    if !status.success() {
        println!("topic creation failed: {}", status);
    }
}

use std::process::Child;

#[allow(unused)]
pub fn command_spawn(binary: &str, prefix: &str, process: impl Fn(&mut Command)) -> Child {
    let mut cmd = get_binary(binary).unwrap_or_else(|_| panic!("unable to get binary: {}", binary));

    let (output, error) = open_log(prefix);

    cmd.stdout(Stdio::from(output)).stderr(Stdio::from(error));

    process(&mut cmd);

    cmd.spawn().expect("child")
}

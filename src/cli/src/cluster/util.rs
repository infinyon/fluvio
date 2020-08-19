use std::io::Error as IoError;
use std::io::ErrorKind;
use std::process::Command;

use tracing::debug;

pub use cmd_util::*;

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
            format!("{} not founded in: {:#?}", bin_name, bin_dir),
        ))
    } else {
        Ok(Command::new(bin_dir.into_os_string()))
    }
}

mod cmd_util {

    pub trait CommandUtil {
        // wait and check
        fn wait(&mut self);

        fn wait_check(&mut self);

        fn print(&mut self) -> &mut Self;

        /// inherit stdout from parent and check for success
        fn inherit(&mut self);
    }

    use std::process::Command;

    impl CommandUtil for Command {
        fn inherit(&mut self) {
            use std::io;
            use std::io::Write;
            use std::process::Stdio;

            self.print();

            let output = self
                .stdout(Stdio::inherit())
                .output()
                .expect("execution failed");

            if !output.status.success() {
                io::stderr().write_all(&output.stderr).unwrap();
            }

            output.status.check();
        }

        /// execute and wait, ignore error
        fn wait(&mut self) {
            use std::io;
            use std::io::Write;

            self.print();

            let output = self.output().expect("execution failed");

            io::stdout().write_all(&output.stdout).unwrap();
            io::stderr().write_all(&output.stderr).unwrap();
        }

        /// execute and wait, ignore error
        fn wait_check(&mut self) {
            use std::io;
            use std::io::Write;

            self.print();

            let output = self.output().expect("execution failed");

            io::stdout().write_all(&output.stdout).unwrap();
            io::stderr().write_all(&output.stderr).unwrap();

            output.status.check();
        }

        fn print(&mut self) -> &mut Self {
            use std::env;

            if env::var_os("FLV_CMD").is_some() {
                println!(">> {}", format!("{:?}", self).replace("\"", ""));
            }

            self
        }
    }

    trait StatusExt {
        fn check(&self);
    }

    impl StatusExt for std::process::ExitStatus {
        fn check(&self) {
            if !self.success() {
                match self.code() {
                    Some(code) => println!("Exited with status code: {}", code),
                    None => println!("Process terminated by signal"),
                }
                unreachable!();
            }
        }
    }
}

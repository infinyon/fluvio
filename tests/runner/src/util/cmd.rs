use std::process::Command;
use std::io;
use std::io::Write;

pub trait CommandUtil {
    fn inherit(&mut self);

    // wait and check
    fn wait_and_check(&mut self);

    // just wait
    fn wait(&mut self);

    fn print(&mut self) -> &mut Self;

    fn log<I>(&mut self, log: Option<I>) -> &mut Self
    where
        I: AsRef<str>;
}

impl CommandUtil for Command {
    fn log<I>(&mut self, log: Option<I>) -> &mut Self
    where
        I: AsRef<str>,
    {
        if let Some(log) = log {
            self.env("RUST_LOG", log.as_ref());
        }
        self
    }

    /// execute and ensure command has been executed ok
    fn inherit(&mut self) {
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

    /// execute and ensure command has been executed ok
    fn wait_and_check(&mut self) {
        self.print();

        let output = self.output().expect("execution failed");

        io::stdout().write_all(&output.stdout).unwrap();
        io::stderr().write_all(&output.stderr).unwrap();

        output.status.check();
    }

    /// execute and wait, ignore error
    fn wait(&mut self) {
        self.print();
        let output = self.output().expect("execution failed");

        io::stdout().write_all(&output.stdout).unwrap();
        io::stderr().write_all(&output.stderr).unwrap();
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
            unreachable!()
        }
    }
}

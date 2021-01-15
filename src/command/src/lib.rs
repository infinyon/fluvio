use std::process::Command;

pub trait CommandExt {
    fn inherit(&mut self) -> &mut Self;
    fn display(&mut self) -> &mut Self;
}

impl CommandExt for Command {
    fn inherit(&mut self) -> &mut Self {
        use std::process::Stdio;
        self.stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
    }

    fn display(&mut self) -> &mut Self {

    }
}

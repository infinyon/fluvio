use std::process::Command;
use std::io;
use std::io::Write;


pub trait CommandUtil {


    // wait and check
    fn wait_and_check(&mut self);

    // just wait
    fn wait(&mut self);

    /// print
    fn print(&mut self) -> &mut Self;

}


impl CommandUtil for Command {

    /// execute and ensure command has been executed ok
    fn wait_and_check(&mut self) {

        let output = self.output().expect("execution failed");

        io::stdout().write_all(&output.stdout).unwrap();
        io::stderr().write_all(&output.stderr).unwrap();

        assert!(output.status.success());

    }

    /// execute and wait, ignore error
    fn wait(&mut self) {

        let output = self.output().expect("execution failed");

        io::stdout().write_all(&output.stdout).unwrap();
        io::stderr().write_all(&output.stderr).unwrap();

    }



    fn print(&mut self) -> &mut Self {

        println!(">> {}",format!("{:?}",self).replace("\"",""));
        self
    }
}
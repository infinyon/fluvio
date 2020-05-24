use std::process::Command;
use std::io;
use std::io::Write;

use crate::Target;
use crate::TlsLoader;

pub trait CommandUtil {

    // set up client tls
    fn setup_client_tls(&mut self,tls: &TlsLoader) -> &mut Self;

    // set up server tls
    fn setup_server_tls(&mut self,tls: &TlsLoader) -> &mut Self;

    // wait and check
    fn wait_and_check(&mut self);

    // just wait
    fn wait(&mut self);

    /// set target including tls
    fn target(&mut self,target: &Target) -> &mut Self;

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

    fn setup_client_tls(&mut self,tls: &TlsLoader) -> &mut Self {

        tls.set_client_tls(self);
        self

    }

    fn setup_server_tls(&mut self,tls: &TlsLoader) -> &mut Self {

        tls.setup_server_tls(self);
        self

    }

    fn target(&mut self,target: &Target) -> &mut Self{

        target.add_to_cmd(self);    
        self
    }
        


    fn print(&mut self) -> &mut Self {

        println!(">> {}",format!("{:?}",self).replace("\"",""));
        self
    }
}
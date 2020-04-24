//!
//! # Produce CLI
//!
//! CLI command for Profile operation
//!

use crate::Terminal;

use crate::CliError;

use structopt::StructOpt;


#[derive(Debug,StructOpt)]
#[structopt(about = "Available Commands")]
pub struct InstallCommand {

    // install release version
    #[structopt(long)]
    release: bool,

    #[structopt(long)]
    version: Option<String>,

    #[structopt(long)]
    namespace: Option<String>,

    /// tls
    #[structopt(long)]
    tls: bool,

    /// RUST_LOG
    #[structopt(long)]
    log: Option<String>
}






pub async fn process_install<O>(_out: std::sync::Arc<O>,command: InstallCommand) -> Result<String,CliError>
    where O: Terminal
 {

    inner_install::invoke_install(command);
    
    Ok("".to_owned())
}

mod inner_install{

    use std::process::Command;
    use std::io;
    use std::io::Write;

    use super::InstallCommand;

    pub fn invoke_install(opt: InstallCommand) {

        let version = if opt.release {
            "1.0".to_owned()
        } else {
            let output = Command::new("git").args(&["log", "-1","--pretty=format:\"%H\""]).output().unwrap();
            let version = String::from_utf8(output.stdout).unwrap();
            version.trim_matches('"').to_owned()
        };

        let registry = if opt.release {
            "infinyon"
        } else {
            "localhost:5000/infinyon"
        };

        let cloud = if opt.release {
            "eks"
        } else {
            "minikube"
        };

        let ns = opt.namespace.unwrap_or("default".to_owned());
        println!("flv: {}",version);

        let fluvio_version = format!("fluvioVersion={}",version);
        println!("using fluvio version: {}",fluvio_version);

        let mut cmd = Command::new("helm");
        cmd
            .arg("install")
            .arg("fluvio")
            .arg("./k8-util/helm/fluvio-core")
            .arg("-n")
            .arg(ns)
            .arg("--set")
            .arg(fluvio_version)
            .arg("--set")
            .arg(format!("registry={}",registry))
            .arg("--set")
            .arg(format!("cloud={}",cloud));
            
        if opt.tls {
            cmd
                .arg("--set")
                .arg("tls=true");
        }

        if let Some(log) = opt.log {
            cmd
                .arg("--set")
                .arg(format!("scLog={}",log));
        }
        
        let output = cmd.output()
            .expect("helm command fail to run");

        io::stdout().write_all(&output.stdout).unwrap();
        io::stderr().write_all(&output.stderr).unwrap();

    }


}
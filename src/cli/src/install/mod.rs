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

    // develop version
    #[structopt(long)]
    develop: bool,

    #[structopt(long)]
    version: Option<String>,

    #[structopt(long,default_value="default")]
    namespace: String,

    #[structopt(long,default_value="fluvio")]
    name: String,

    #[structopt(long,default_value="minikube")]
    cloud: String,

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

        let version = if opt.develop {
            // get git version
            let output = Command::new("git").args(&["log", "-1","--pretty=format:\"%H\""]).output().unwrap();
            let version = String::from_utf8(output.stdout).unwrap();
            version.trim_matches('"').to_owned()
        } else {
            crate::VERSION.to_owned()
        };

        let registry = if opt.develop {
            "localhost:5000/infinyon"
        } else {
            "infinyon"
        };

        
        let ns = opt.namespace;

        println!("flv: {}",version);

        let fluvio_version = format!("fluvioVersion={}",version);
        println!("using fluvio version: {}",fluvio_version);

        let mut cmd = Command::new("helm");

        if opt.develop {
            cmd
                .arg("install")
                .arg(opt.name)
                .arg("./k8-util/helm/fluvio-core")
                .arg("--set")
                .arg(fluvio_version)
                .arg("--set")
                .arg(format!("registry={}",registry));
        } else {

            helm_add_repo();
            helm_repo_update();

            cmd
                .arg("install")
                .arg(opt.name)
                .arg("fluvio/fluvio-core");
        };

        cmd
            .arg("-n")
            .arg(ns)
            .arg("--set")
            .arg(format!("cloud={}",opt.cloud));
            
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

    fn helm_add_repo() {

        // add repo
        let output = Command::new("helm")
            .arg("repo")
            .arg("add")
            .arg("fluvio")
            .arg("https://infinyon.github.io/charts")
            .output()
            .expect("adding repo");

        io::stdout().write_all(&output.stdout).unwrap();
        io::stderr().write_all(&output.stderr).unwrap();

        assert!(output.status.success());
    }

    fn helm_repo_update() {

        // add repo
        let output = Command::new("helm")
            .arg("repo")
            .arg("update")
            .output()
            .expect("adding repo");

        io::stdout().write_all(&output.stdout).unwrap();
        io::stderr().write_all(&output.stderr).unwrap();

        assert!(output.status.success());

    }


}
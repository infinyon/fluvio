use structopt::StructOpt;

use log::debug;

use crate::CliError;

pub use context::process_minikube_context;

#[derive(Debug,StructOpt)]
pub struct SetMinikubeContext {

    /// set context name
    #[structopt(long,value_name = "name")]
    pub name: Option<String>,
}




mod context {

    use super::*;

    const TEMPLATE: &'static str = r#"
    #!/bin/bash
    export IP=$(minikube ip)
    sudo sed -i '' '/minikubeCA/d' /etc/hosts
    echo "$IP minikubeCA" | sudo tee -a  /etc/hosts
    cd ~
    kubectl config set-cluster {{ name }} --server=https://minikubeCA:8443 --certificate-authority=.minikube/ca.crt
    kubectl config set-context {{ name }} --user=minikube --cluster={{ name }}
    kubectl config use-context {{ name }}
    "#;


    /// Performs following
    ///     add minikube IP address to /etc/host
    ///     create new kubectl cluster and context which uses minikube name
    pub fn process_minikube_context(ctx: SetMinikubeContext) -> Result<String,CliError> {

        use std::io::Write;
        use std::io;
        use std::os::unix::fs::OpenOptionsExt;
        use std::fs::OpenOptions;
        use std::process::Command;
        use std::env;

        use tera::Tera;
        use tera::Context;

        let mut tera = Tera::default();

        let name = ctx.name.unwrap_or("mycube".to_owned());
        tera.add_raw_template("cube.sh", TEMPLATE).expect("string compilation");

        let mut context = Context::new();
        context.insert("name", &name);

        let render = tera.render("cube.sh", &context).expect("rendering");

        let tmp_file = env::temp_dir().join("flv_minikube.sh");

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .mode(0o755)
            .open(tmp_file.clone())?;

        file.write_all(render.as_bytes())?;

        debug!("script {}",render);

        let output = Command::new(tmp_file).output().expect("cluster command");
        io::stdout().write_all(&output.stdout).unwrap();
        io::stderr().write_all(&output.stderr).unwrap();


        Ok("".to_owned())
    }
}
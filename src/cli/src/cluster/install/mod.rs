use crate::Terminal;

use crate::CliError;

use structopt::StructOpt;

use super::CommandUtil;
use k8_util::*;

#[derive(Debug,StructOpt)]
pub struct InstallCommand {

    // develop version
    #[structopt(long)]
    develop: bool,

    #[structopt(long)]
    version: Option<String>,

    #[structopt(long,default_value="default")]
    namespace: String,

    #[structopt(long,default_value="main")]
    group_name: String,

   
    #[structopt(long,default_value="fluvio")]
    name: String,

    #[structopt(long,default_value="minikube")]
    cloud: String,

    /// number of spu
    #[structopt(long,default_value="1")]
    spu: u16,

    /// tls
    #[structopt(long)]
    tls: bool,

    /// RUST_LOG
    #[structopt(long)]
    log: Option<String>,

    #[structopt(long)]
    /// installing sys
    sys: bool
}



pub async fn process_install<O>(_out: std::sync::Arc<O>,command: InstallCommand) -> Result<String,CliError>
    where O: Terminal
 {

    if command.sys {
        inner_install::install_sys(command);
    } else {
        inner_install::install_core(command).await?;
    }
    
    
    Ok("".to_owned())
}

mod inner_install{

    use std::process::Command;
    use std::io::Error as IoError;

    use super::*;

    pub async fn install_core(opt: InstallCommand) -> Result<(),CliError> {

        install_core_app(&opt)?;

        if let Some(_) = wait_for_service_exist(&opt).await
                    .map_err(|err| CliError::Other(err.to_string()))? 
        {
            println!("fluvio is up");
            set_profile(&opt).await?;

            if opt.spu > 0 {
                create_spg(&opt).await?;
            }
           
            Ok(())
        } else {
            println!("unable to detect fluvio service");
            println!("for minikube, check if you have tunnel up!");
            Err(CliError::Other("unable to detect fluvio service".to_owned()))
        }

        
       
    }

    /// install helm core chart
    fn install_core_app(opt: &InstallCommand) -> Result<(),CliError>  {

        

        let version = if opt.develop {
            // get git version
            let output = Command::new("git").args(&["log", "-1","--pretty=format:\"%H\""]).output().unwrap();
            let version = String::from_utf8(output.stdout).unwrap();
            version.trim_matches('"').to_owned()
        } else {
            helm_add_repo();
            helm_repo_update();
            crate::VERSION.to_owned()
        };

        let registry = if opt.develop {
            "localhost:5000/infinyon"
        } else {
            "infinyon"
        };

        
        let ns = &opt.namespace;

        println!("flv: {}",version);

        let fluvio_version = format!("fluvioVersion={}",version);
        println!("using fluvio version: {}",fluvio_version);

        let mut cmd = Command::new("helm");

        if opt.develop {
            cmd
                .arg("install")
                .arg(&opt.name)
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
                .arg(&opt.name)
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

        if let Some(log) = &opt.log {
            cmd
                .arg("--set")
                .arg(format!("scLog={}",log));
        }

        cmd.wait();
        
        println!("fluvio chart has been installed");

        Ok(())

    }

    pub fn install_sys(opt: InstallCommand) {

        
        helm_add_repo();
        helm_repo_update();
       
        Command::new("helm")
            .arg("install")
            .arg("fluvio-sys")
            .arg("fluvio/fluvio-sys")
            .arg("--set")
            .arg(format!("cloud={}",opt.cloud))
            .wait_check();


        println!("fluvio sys chart has been installed");

    }

    fn helm_add_repo() {

        // add repo
        Command::new("helm")
            .arg("repo")
            .arg("add")
            .arg("fluvio")
            .arg("https://infinyon.github.io/charts")
            .wait_check();
    }

    fn helm_repo_update() {

        // add repo
        Command::new("helm")
            .arg("repo")
            .arg("update")
            .wait_check();
    }

    /// switch to profile
    async fn set_profile(opt: &InstallCommand) -> Result<(),IoError> {

        use crate::profile::set_k8_context;
        use crate::profile::SetK8;

        let config =  SetK8 {
            namespace: Some(opt.namespace.clone()),
            ..Default::default()   
        };

        println!("{}",set_k8_context(config).await?);

        Ok(())
    }


    async fn create_spg(opt: &InstallCommand) -> Result<(),CliError> {

        use crate::spu::group::process_create_managed_spu_group;
        use crate::spu::group::CreateManagedSpuGroupOpt;

        let group_opt = CreateManagedSpuGroupOpt {
            name: opt.group_name.clone(),
            replicas: opt.spu,
            ..Default::default()
        };

        process_create_managed_spu_group(group_opt).await?;

        println!("group: {} with replica: {} created",opt.group_name,opt.spu);

        Ok(())
    }


}


mod k8_util {

    use std::time::Duration;


    use flv_future_aio::timer::sleep;
    use k8_client::ClientError;
    use k8_client::load_and_share;
    use k8_obj_core::service::ServiceSpec;
    use k8_obj_metadata::InputObjectMeta;
    use k8_metadata_client::MetadataClient;
    use k8_client::ClientError as K8ClientError;

    use super::*;

   
    pub async fn wait_for_service_exist(opt: &InstallCommand) -> Result<Option<String>, ClientError>  {

        let client = load_and_share()?;

        let input = InputObjectMeta::named("flv-sc-public", &opt.namespace);

        for i in 0..100u16 {
            println!("checking to see if svc exists, count: {}",i);
            match client.retrieve_item::<ServiceSpec,_>(&input).await {
                Ok(svc) => {
                    // check if load balancer status exists
                    if let Some(addr) = svc.status.load_balancer.find_any_ip_or_host() {
                        println!("found svc load balancer addr: {}",addr);
                        return Ok(Some(format!("{}:9003",addr.to_owned())))
                    } else {
                        println!("svc exists but no load balancer exist yet, continue wait");
                        sleep(Duration::from_millis(3000)).await;
                    }
                },
                Err(err) => match err {
                    K8ClientError::NotFound => {
                        println!("no svc found, sleeping ");
                        sleep(Duration::from_millis(3000)).await;
                    },
                    _ => assert!(false,format!("error: {}",err))
                }
            };
        }

        Ok(None)

    }

    
}

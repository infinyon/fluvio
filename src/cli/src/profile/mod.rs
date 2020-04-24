mod cli;
mod k8;

use std::io::Error as IoError;
use std::io::ErrorKind;

use cli::Command;
use log::debug;

use flv_client::profile::ConfigFile;
use flv_client::profile::Cluster;
use flv_client::profile::Profile;
use flv_client::profile::LOCAL_PROFILE;

pub use cli::ProfileCommand;

use crate::Terminal;
use crate::t_println;
use crate::t_print_cli_err;

use crate::error::CliError;



pub async fn process_profile<O>(out: std::sync::Arc<O>,profile_command: ProfileCommand) -> Result<String,CliError>
    where O: Terminal
 {

    let cmd = profile_command.cmd;
    match cmd {
        Command::View =>  view_profile(out.clone()),
        Command::DisplayCurrentProfile => display_current_profile(out.clone()),
        Command::UseProfile(profile) => {
            match ConfigFile::load(None) {
                Ok(mut config_file) => {
                    if !config_file.mut_config().set_current_profile(&profile.profile_name) {
                        t_println!(out,"profile {} not founded",config_file.config().current_profile_name());
                    } else {
                        if let Err(err) = config_file.save() {
                            t_println!(out,"unable to save profile: {}",err);
                        }
                    }
                   
                },
                Err(_) => {
                    t_print_cli_err!(out,"no profile can be founded")
                }
            }
        },
        Command::SetLocalProfile(opt) => match set_local_context(opt) {
            Ok(msg) => t_println!(out,"{}",msg),
            Err(err) =>  {
                eprintln!("config creation failed: {}",err);
            }
        },
        Command::SetK8Profile(opt) => match k8_context::set_k8_context(opt).await {
            Ok(msg) => t_println!(out,"{}",msg),
            Err(err) =>  {
                eprintln!("config creation failed: {}",err);
            }
        },
    }
    
    Ok("".to_owned())
}

/// create new local cluster and profile
fn set_local_context(local_config: cli::SetLocal) -> Result<String,IoError> {


    let local_addr = local_config.local;
    let mut config_file = ConfigFile::load_default_or_new()?;

    let config = config_file.mut_config();

    // check if local cluster exists otherwise, create new one
    match config.mut_cluster(LOCAL_PROFILE) {
        Some(cluster) =>  {
            cluster.addr = local_addr.clone();
            cluster.tls =  local_config.tls.try_into_inline()?;
        },
        None =>  {
            let mut local_cluster = Cluster::new(local_addr.clone());
            local_cluster.tls =  local_config.tls.try_into_inline()?;
            config.add_cluster(local_cluster,LOCAL_PROFILE.to_owned());
        }
    };

    

    // check if we local profile exits otherwise, create new one, then set it's cluster
    match config.mut_profile(LOCAL_PROFILE) {
        Some(profile) => {
            profile.set_cluster(LOCAL_PROFILE.to_owned());
        },
        None =>  {
            let profile = Profile::new(LOCAL_PROFILE.to_owned());
            config.add_profile(profile,LOCAL_PROFILE.to_owned());
        }
    }

    // finally we set current profile to local
    assert!(config.set_current_profile(LOCAL_PROFILE));

    config_file.save()?;
    

    Ok(format!("local context is set to: {}",local_addr))
    
}


mod k8_context {

    use super::*;

    use k8_client::K8Config;
    use k8_client::K8Client;

    /// compute profile name, if name exists in the cli option, we use that
    /// otherwise, we look up k8 config context name
    fn compute_profile_name(opt: &cli::SetK8, k8_config: &K8Config) -> Result<String,IoError> {

        if let Some(name) = &opt.name {
            return Ok(name.to_owned())
        }

        let kc_config = match k8_config {
            K8Config::Pod(_) => return Err(IoError::new(ErrorKind::Other, "Pod config is not valid here")),
            K8Config::KubeConfig(config) => config
        };

        if let Some(ctx) = kc_config.config.current_context() {
            Ok(ctx.name.to_owned())
        } else {
            Err(IoError::new(ErrorKind::Other, "no context found"))
        }

    }

    /// create new k8 cluster and profile
    pub async fn set_k8_context(opt: cli::SetK8) -> Result<String,IoError> {

        let mut config_file = ConfigFile::load_default_or_new()?;

        let k8_config = K8Config::load()
            .map_err(|err| IoError::new(ErrorKind::Other, format!("unable to load kube context {}", err)))?;

        let profile_name = compute_profile_name(&opt, &k8_config)?;

        let k8_client = K8Client::new(k8_config)
            .map_err(|err| IoError::new(ErrorKind::Other, format!("unable to create kubernetes client: {}",err)))?;

        let external_addr = if let Some(sc_addr) =   k8::discover_fluvio_addr(&k8_client, opt.namespace).await?{
            sc_addr
        } else {
            return Err(IoError::new(ErrorKind::Other, format!("fluvio service is not deployed")))
        };

        debug!("found sc_addr is: {}",external_addr);

        let config = config_file.mut_config();


        match config.mut_cluster(&profile_name) {
            Some(cluster) =>  {
                cluster.set_addr(external_addr.clone());
                cluster.tls = opt.tls.try_into_inline()?;
            },
            None =>  {
                let mut local_cluster = Cluster::new(external_addr.clone());
                local_cluster.tls = opt.tls.try_into_inline()?;
                config.add_cluster(local_cluster,profile_name.clone());
            }
        };
    
        // check if we local profile exits otherwise, create new one, then set name as cluster
        match config.mut_profile(&profile_name) {
            Some(profile) => {
                profile.set_cluster(profile_name.clone());
            },
            None =>  {
                let profile = Profile::new(profile_name.clone());
                config.add_profile(profile,profile_name.clone());
            }
        }
    
        // finally we set current profile to local
        assert!(config.set_current_profile(&profile_name));


        config_file.save()?;
        
        Ok(format!("new cluster/profile: {} is set to: {}",profile_name,external_addr))
    }
}


fn view_profile<O>(out: std::sync::Arc<O>)
    where O: Terminal
  {

    match ConfigFile::load(None) {
        Ok(config_file) => {
            t_println!(out,"{:#?}",config_file.config())
        },
        Err(_) => {
            t_println!(out,"no profile can be founded")
        }
    }
}

fn display_current_profile<O>(out: std::sync::Arc<O>)
    where O: Terminal
  {

    match ConfigFile::load(None) {
        Ok(config_file) => {
            t_println!(out,"{}",config_file.config().current_profile_name())
        },
        Err(_) => {
            t_println!(out,"no profile can be founded")
        }
    }
}



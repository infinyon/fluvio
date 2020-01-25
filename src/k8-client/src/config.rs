
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::path::Path;


use log::debug;
use log::trace;
use isahc::HttpClient;
use isahc::HttpClientBuilder;
use isahc::config::ClientCertificate;
use isahc::config::PrivateKey;
use isahc::config::CaCertificate;

use k8_config::K8Config;
use k8_config::PodConfig;
use k8_config::KubeConfig;

use crate::ClientError;

/// load client certificate 
fn load_client_certificate<P>(client_crt_path: P,client_key_path: P) -> ClientCertificate 
    where P: AsRef<Path>
{
    ClientCertificate::pem_file(
        client_crt_path.as_ref().to_owned(),
        PrivateKey::pem_file(client_key_path.as_ref().to_owned(), String::from("")),
    )
}

fn load_ca_certificate<P>(ca_path: P) -> CaCertificate 
    where P: AsRef<Path>
{
    CaCertificate::file(ca_path.as_ref().to_owned())
}




/// Build and configure HTTP connection for K8
#[derive(Debug)]
pub struct K8HttpClientBuilder(K8Config);

impl K8HttpClientBuilder {

    pub fn new(config: K8Config) -> Self {
        Self(config)
    }

    pub fn config(&self) -> &K8Config {
        &self.0
    }

    /// build HttpClient, for now we use isahc
    pub fn build(&self) -> Result<HttpClient, ClientError> {

        let builder = HttpClient::builder();

        let builder = match &self.0 {
            K8Config::Pod(pod_config) => configure_in_cluster(pod_config,builder),
            K8Config::KubeConfig(kube_config) => configure_out_of_cluster(&kube_config.config,builder),
        }?;

        builder.build().map_err(|err| err.into())
    }

    /// get any additional auth token that is required
    pub fn token(&self) -> Option<String> {
        match &self.0 {
            K8Config::Pod(pod) =>  Some(pod.token.to_owned()),
            _ => None
        }
    }
}

/// Configure builder if we are out of cluster, 
/// in this case, we need to use kubeconfig to get certificate locations
fn configure_out_of_cluster(kube_config: &KubeConfig, builder: HttpClientBuilder) -> Result<HttpClientBuilder,IoError>  {

    let current_user = kube_config.current_user().ok_or_else(
        || IoError::new(ErrorKind::InvalidInput,"config must have current user".to_owned()))?;
  
    // get certs for client-certificate
    let client_crt_path = current_user.user.client_certificate.as_ref().ok_or_else(
        || IoError::new(ErrorKind::InvalidInput,"no client cert crt path founded".to_owned()))?;
    
    let client_key_path = &current_user.user.client_key.as_ref().ok_or_else(
        || IoError::new(ErrorKind::InvalidInput,"no client cert key founded".to_owned()))?;
    
    trace!("loading client crt: {} and client key: {}",client_crt_path,client_key_path);

    let client_certificate = load_client_certificate(client_crt_path,client_key_path);
        
    debug!("retrieved client certs from kubeconfig");
    let builder = builder.ssl_client_certificate(client_certificate);
        
    let current_cluster = kube_config.current_cluster().ok_or_else(
        || IoError::new(ErrorKind::InvalidInput,"config must have current cluster".to_owned()))?;

    let ca_certificate_path = current_cluster.cluster.certificate_authority.as_ref().ok_or_else(
        || IoError::new(ErrorKind::InvalidInput,"current cluster must have CA crt path".to_owned()))?;

    let ca_certificate = load_ca_certificate(ca_certificate_path);

    Ok(builder.ssl_ca_certificate(ca_certificate))
}


/// Configure builder if it is in cluster
/// we need to get token and configure client
fn configure_in_cluster(pod: &PodConfig,builder: HttpClientBuilder) -> Result<HttpClientBuilder,IoError> { 
   
    
    let ca_certificate = load_ca_certificate(pod.ca_path());
    
    debug!("retrieve ca.crt");
    Ok(builder.ssl_ca_certificate(ca_certificate))
}




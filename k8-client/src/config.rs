
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::fs::File;
use std::path::Path;
use std::io::Read;
use std::io::BufReader;
use std::sync::Arc;


use log::debug;
use log::trace;
use hyper_rustls::HttpsConnector;
use rustls::ClientConfig;
use rustls::PrivateKey;
use rustls::Certificate;
use rustls::ServerCertVerifier;
use rustls::RootCertStore;
use rustls::ServerCertVerified;
use rustls::internal::pemfile::certs;
use rustls::internal::pemfile::rsa_private_keys;
use rustls::TLSError;
use hyper::client::HttpConnector;
use webpki::DNSNameRef;


use k8_config::K8Config;
use k8_config::PodConfig;
use k8_config::KubeConfig;


fn retrieve_cert<R>(reader: R) -> Result<Vec<Certificate>, IoError> where R: Read {
    let mut reader = BufReader::new(reader);
    certs(&mut reader).map_err(|_| IoError::new(ErrorKind::Other, format!("no cert found")))
}

fn retrieve_cert_from_file<P>(file_path: P) -> Result<Vec<Certificate>, IoError>
    where P: AsRef<Path>
{
    let file = File::open(file_path)?;
    retrieve_cert(file)
}

fn retrieve_private_key<P>(filename: P) -> Result<Vec<PrivateKey>,IoError> 
    where P: AsRef<Path>
{
    let keyfile = File::open(filename)?;
    let mut reader = BufReader::new(keyfile);
    rsa_private_keys(&mut reader).map_err(|_| IoError::new(
            ErrorKind::InvalidData,
            "private key not founded"
    ))
}



struct NoVerifier {}

impl ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _roots: &RootCertStore,
        _presented_certs: &[Certificate],
        dns_name: DNSNameRef,
        _ocsp_response: &[u8],
    ) -> Result<ServerCertVerified, TLSError> {

        trace!("decoding dns: {:#?}",dns_name);
        Ok(ServerCertVerified::assertion())
    }
}


#[derive(Debug)]
pub struct K8AuthHelper {
    pub config: K8Config
}

impl K8AuthHelper {

    pub fn new(config: K8Config) -> Self {
        Self {
            config
        }
    }

    pub fn build_https_connector(&self) -> Result<HttpsConnector<HttpConnector>, IoError> {

        match &self.config {
            K8Config::Pod(pod_config) => build_token_connector(&pod_config),
            K8Config::KubeConfig(kube_config) => build_client_cert_connector(&kube_config.config)
        }
    }
}

fn build_client_cert_connector(kube_config: &KubeConfig) -> Result<HttpsConnector<HttpConnector>, IoError> {

    let mut tls = ClientConfig::new();
    let mut http = HttpConnector::new(1);
    http.enforce_http(false);

    let user = kube_config.current_user().unwrap();

    // get certs for client-certificate
    if let Some(client_certificate) = &user.user.client_certificate {
        let client_certs = retrieve_cert_from_file(&client_certificate)?;
        debug!("client certs: {:#?}",client_certs);
        let mut private_keys = retrieve_private_key(user.user.client_key.as_ref().expect("client key expected"))?;

        if private_keys.len() == 0 {
            return Err(IoError::new(
                ErrorKind::InvalidData,
                "private key not founded"
            ))
        }

        debug!("retrieved client certs from kubeconfig");
        tls.set_single_client_cert(client_certs, private_keys.remove(0));
        tls.dangerous()
            .set_certificate_verifier(Arc::new(NoVerifier {}));
        Ok(HttpsConnector::from((http, tls)))
    } else {
        Err(IoError::new(ErrorKind::InvalidInput,"no certificate founded".to_owned()))
    }
    
}

fn build_token_connector(pod: &PodConfig) -> Result<HttpsConnector<HttpConnector>, IoError> { 
   
    let mut http = HttpConnector::new(1);
    http.enforce_http(false);
    let mut tls = ClientConfig::new();
    
    for cert in retrieve_cert(pod.ca.as_bytes())? {
        tls.root_store
            .add(&cert)
            .map_err(|err| {
                IoError::new(ErrorKind::Other, format!("cert error: {:#?}", err))
            })
            .expect("problem reading cert");
    }

    debug!("retrieve ca.crt for token authenication");
    Ok(HttpsConnector::from((http, tls)))
}



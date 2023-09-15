use std::{
    fs::File,
    path::{PathBuf, Path},
    process::{Command, Stdio},
};

use rcgen::{Certificate, CertificateParams, IsCa, DistinguishedName, DnType, DnValue, KeyUsagePurpose};
use tracing::{info, debug};

use fluvio_command::CommandExt;
use k8_config::{K8Config, User, UserDetail, Cluster, ClusterDetail, Context, ContextDetail};

use crate::pick_unused_port;

use super::LocalRuntimeError;

const DEFAULT_K8S_CONTEXT_NAME: &str = "fluvio-local";
const DEFAULT_K8S_CLUSTER_NAME: &str = "fluvio-local-cluster";
const DEFAULT_K8S_USER_NAME: &str = "fluvio-local-client";
const DEFAULT_K8S_NAMESPACE: &str = "default";
const CERT_DOMAINS: &[&str] = &["127.0.0.1", "localhost"];

pub(crate) struct K8sProcess {
    pub log_dir: PathBuf,
    pub data_dir: PathBuf,
    pub base: PathBuf,
    pub etcd_endpoint_url: String,
}

impl K8sProcess {
    pub fn start(&self) -> Result<(String, Certs), LocalRuntimeError> {
        let logs_path = self.log_dir.join("flv_k8s.log");
        let outputs = File::create(&logs_path)?;
        let errors = outputs.try_clone()?;

        let port = pick_unused_port()?;
        let bind_addr = "127.0.0.1";

        let certs = generate_certificates(&self.data_dir.join("k8s/certs")).map_err(|err| {
            LocalRuntimeError::Other(format!("unable to generate certificates for k8s: {err}"))
        })?;

        let mut binary = {
            let mut cmd = Command::new(&self.base);
            let Certs {
                root_cert,
                kube_private_key,
                kube_cert,
                service_account_private_key,
                service_account_cert,
                client_private_key: _,
                client_cert: _,
            } = &certs;
            cmd.arg("k8s")
                .arg("--storage-backend=etcd3")
                .arg(format!("--etcd-servers={}", self.etcd_endpoint_url))
                .arg(format!("--bind-address={bind_addr}"))
                .arg(format!("--secure-port={port}"))
                .arg(format!("--client-ca-file={}", root_cert.to_string_lossy()))
                .arg("--service-cluster-ip-range=127.0.0.0/16")
                .args(["--api-audiences", "api "])
                .arg("--service-account-issuer=api")
                .arg(format!(
                    "--service-account-signing-key-file={}",
                    service_account_private_key.to_string_lossy()
                ))
                .arg(format!(
                    "--service-account-key-file={}",
                    service_account_cert.to_string_lossy()
                ))
                .arg(format!("--tls-cert-file={}", kube_cert.to_string_lossy()))
                .arg(format!(
                    "--tls-private-key-file={}",
                    kube_private_key.to_string_lossy()
                ));
            cmd
        };
        info!(cmd = %binary.display(), logs = logs_path.to_string_lossy().to_string(), "Invoking command");
        binary
            .stdout(Stdio::from(outputs))
            .stderr(Stdio::from(errors))
            .spawn()?;

        let endpoint = format!("https://{bind_addr}:{port}");

        set_k8s_context(&certs, &endpoint)
            .map_err(|err| LocalRuntimeError::Other(format!("unable to set k8s context: {err}")))?;

        Ok((endpoint, certs))
    }
}

fn set_k8s_context(certs: &Certs, endpoint: &str) -> anyhow::Result<()> {
    match K8Config::load()? {
        K8Config::Pod(_) => anyhow::bail!("unexpected k8s config type: Pod"),
        K8Config::KubeConfig(kube_context) => {
            let mut config = kube_context.config;
            let user = User {
                name: DEFAULT_K8S_USER_NAME.to_string(),
                user: UserDetail {
                    client_certificate: Some(certs.client_cert.to_string_lossy().to_string()),
                    client_key: Some(certs.client_private_key.to_string_lossy().to_string()),
                    ..Default::default()
                },
            };

            let cluster = Cluster {
                name: DEFAULT_K8S_CLUSTER_NAME.to_string(),
                cluster: ClusterDetail {
                    certificate_authority: Some(certs.root_cert.to_string_lossy().to_string()),
                    server: endpoint.to_string(),
                    ..Default::default()
                },
            };

            let namespace = DEFAULT_K8S_NAMESPACE;
            let context = Context {
                name: DEFAULT_K8S_CONTEXT_NAME.to_string(),
                context: ContextDetail {
                    cluster: cluster.name.clone(),
                    user: user.name.clone(),
                    namespace: Some(namespace.to_string()),
                },
            };

            config.current_context = context.name.clone();
            config.put_user(user);
            config.put_cluster(cluster);
            config.put_context(context);

            config.save()?;

            info!(
                path = ?config.path.to_string_lossy(),
                "k8s context succesfully added"
            );

            Ok(())
        }
    }
}

fn generate_certificates(certs_path: &Path) -> anyhow::Result<Certs> {
    let _ = std::fs::create_dir_all(certs_path);
    let kube_private_key = certs_path.join("kubernetes-key.pem");
    let kube_cert = certs_path.join("kubernetes.pem");
    let service_account_private_key = certs_path.join("service-account-key.pem");
    let service_account_cert = certs_path.join("service-account.pem");
    let client_private_key = certs_path.join("client-key.pem");
    let client_cert = certs_path.join("client.pem");

    let root_cert = certs_path.join("ca.pem");
    let root_private_key = certs_path.join("ca-key.pem");
    if !root_cert.exists() {
        info!("regenerating k8s certificates");
        let root = generate_root_cert(&root_private_key, &root_cert)?;

        generate_cert(&root, &kube_private_key, &kube_cert)?;
        generate_cert(&root, &service_account_private_key, &service_account_cert)?;
        generate_cert(&root, &client_private_key, &client_cert)?;
    }

    Ok(Certs {
        kube_private_key,
        kube_cert,
        service_account_private_key,
        service_account_cert,
        root_cert,
        client_private_key,
        client_cert,
    })
}

fn generate_root_cert(private_key: &Path, public_key: &Path) -> anyhow::Result<Certificate> {
    let mut dn = DistinguishedName::new();
    dn.push(DnType::OrganizationName, "fluvio.io");
    dn.push(
        DnType::CommonName,
        DnValue::PrintableString("fluvio.io local ca cert".to_string()),
    );
    let mut root_params = CertificateParams::new(
        CERT_DOMAINS
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<String>>(),
    );
    root_params.is_ca = IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    root_params.distinguished_name = dn;
    root_params.key_usages = vec![KeyUsagePurpose::CrlSign, KeyUsagePurpose::KeyCertSign];
    let cert = Certificate::from_params(root_params)?;
    debug!(?private_key, "writing to root private key");
    std::fs::write(private_key, cert.serialize_private_key_pem())?;
    debug!(?public_key, "writing to root public key");
    std::fs::write(public_key, cert.serialize_pem()?)?;
    Ok(cert)
}

fn generate_cert(
    root: &Certificate,
    private_key: &Path,
    public_key: &Path,
) -> anyhow::Result<Certificate> {
    let cert = rcgen::generate_simple_self_signed(
        CERT_DOMAINS
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<String>>(),
    )?;
    debug!(?private_key, "writing to private key");
    std::fs::write(private_key, cert.serialize_private_key_pem())?;
    debug!(?public_key, "writing to public key");
    std::fs::write(public_key, cert.serialize_pem_with_signer(root)?)?;
    Ok(cert)
}

pub(crate) struct Certs {
    pub root_cert: PathBuf,
    pub kube_private_key: PathBuf,
    pub kube_cert: PathBuf,
    pub service_account_private_key: PathBuf,
    pub service_account_cert: PathBuf,
    pub client_private_key: PathBuf,
    pub client_cert: PathBuf,
}

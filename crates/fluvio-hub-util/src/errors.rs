use crate::infinyon_tok::InfinyonCredentialError;

#[derive(thiserror::Error, Debug)]
pub enum HubUtilError {
    #[error(transparent)]
    CargoReadError(#[from] cargo_toml::Error),

    #[error("No package section in Cargo.toml")]
    CargoMissingPackageSection,

    #[error("General Error: {0}")]
    General(String),

    #[error("Hub access: {0}")]
    HubAccess(String),

    #[error("Invalid keypair file: {0}")]
    InvalidKeyPairFile(String),

    #[error("Invalid package name: {0}")]
    InvalidPackageName(String),

    #[error("Invalid public key file: {0}")]
    InvalidPublicKeyFile(String),

    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error("Json Serialization error {0}")]
    JsonSerializationError(#[from] serde_json::Error),

    #[error("key verify error")]
    KeyVerify,

    #[error("Invalid filename in package-meta: {0}")]
    ManifestInvalidFile(String),

    #[error("Missing file in package-meta: {0}")]
    ManifestMissingFile(String),

    #[error("Downloading package {0}")]
    PackageDownload(String),

    #[error("Publishing package {0}")]
    PackagePublish(String),

    #[error("Signing package {0}")]
    PackageSigning(String),

    #[error("Package verification {0}")]
    PackageVerify(String),

    #[error("Unable to package: {0}")]
    UnableToAssemblePackage(String),

    #[error("Unable to access package-meta in: {0}")]
    UnableGetPackageMeta(String),

    #[error(transparent)]
    UnableToReadCredentials(#[from] InfinyonCredentialError),

    #[error("signature error")]
    SignatureError,

    #[error("Error processing yaml file")]
    YamlError(#[from] serde_yaml::Error),
}

pub type Result<T> = std::result::Result<T, HubUtilError>;

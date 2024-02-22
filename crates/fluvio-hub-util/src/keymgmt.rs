//! Key Mgmt module provides a thin abstraction over the public key
//! signing and validation ed25519-dalek crate
//! ed25519-dalek is a common crate between jwt token crates, package signing/verification
//! and ssh key representations.

use std::io::Write;

use ed25519_dalek::{Signer, Verifier};
use pem::Pem;

use fluvio_hub_protocol::{HubError, Result};

const PRIVATE_KEY_TAG: &str = "PRIVATE KEY";
const PUBLIC_KEY_TAG: &str = "PUBLIC KEY";

// keypair containing private and public keys
#[derive(Clone)]
pub struct Keypair {
    kp: ed25519_dalek::SigningKey,
}

// Add a debug impl that hides the contents to make errors a little easier
// to work with
impl std::fmt::Debug for Keypair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "Keypair (*)")
    }
}

#[derive(Clone, Debug)]
pub struct PublicKey {
    pubkey: ed25519_dalek::VerifyingKey,
}

pub type Signature = ed25519_dalek::Signature;

impl Keypair {
    pub fn new() -> Result<Keypair> {
        use rand::rngs::OsRng;
        let mut csprng = OsRng;
        Ok(Keypair {
            kp: ed25519_dalek::SigningKey::generate(&mut csprng),
        })
    }

    /// sign the message, generally files in the package
    pub fn sign(&self, buf: &[u8]) -> Result<Signature> {
        let sig = self
            .kp
            .try_sign(buf)
            .map_err(|_| HubError::SignatureError)?;
        Ok(sig)
    }

    pub fn public(&self) -> PublicKey {
        PublicKey {
            pubkey: self.kp.verifying_key(),
        }
    }

    /// read private key and derive public to populate keypair
    pub fn read_from_file(fname: &str) -> Result<Keypair> {
        let buf = std::fs::read(fname)?;
        let pem = pem::parse(buf).map_err(|_| HubError::InvalidKeyPairFile(fname.into()))?;
        if pem.tag() != PRIVATE_KEY_TAG {
            return Err(HubError::InvalidKeyPairFile(fname.into()));
        }
        Keypair::from_secret_bytes(pem.contents())
    }

    /// writes the private key from which the public is derivable on load
    pub fn write_keypair(&self, fname: &str) -> Result<()> {
        let pem = Pem::new(PRIVATE_KEY_TAG, self.kp.as_bytes().to_vec());
        let buf = pem::encode(&pem);
        let mut file = std::fs::File::create(fname)?;
        set_perms_owner_rw(&mut file)?;
        file.write_all(buf.as_bytes())?;
        Ok(())
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.kp.as_bytes())
    }

    pub fn from_hex(hexstring: &str) -> Result<Keypair> {
        let pkbytes = hex::decode(hexstring).map_err(|_| HubError::KeyVerify)?;
        Keypair::from_secret_bytes(&pkbytes)
    }

    /// a pubkey from an ssh private key (id_ed25519) generated via
    ///  e.g. $ ssh-keygen -t ed25519 -C "sshkey@example.com" -f ./id_ed25519 -P ""
    /// ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIACGeGHWvt/E60k/FLuDsCkArLAIa4lvwk1wg3nJIGJl sshkey@example.com
    pub fn from_ssh(env_val: &str) -> Result<Keypair> {
        let sshprivkey =
            ssh_key::PrivateKey::from_openssh(env_val).map_err(|_| HubError::KeyVerify)?;
        let keypair = sshprivkey.key_data().ed25519().ok_or(HubError::KeyVerify)?;
        Keypair::from_secret_bytes(keypair.private.as_ref())
    }

    fn from_secret_bytes(sbytes: &[u8]) -> Result<Keypair> {
        let skey = sbytes.try_into().map_err(|_| HubError::KeyVerify)?;
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&skey);
        Ok(Keypair { kp: signing_key })
    }

    pub fn ref_dalek(&self) -> &ed25519_dalek::SigningKey {
        &self.kp
    }
}

impl PublicKey {
    pub fn read_from_file(fname: &str) -> Result<PublicKey> {
        let buf = std::fs::read(fname)?;
        let pem = pem::parse(buf).map_err(|_| HubError::InvalidPublicKeyFile(fname.into()))?;
        if pem.tag() != PUBLIC_KEY_TAG {
            return Err(HubError::InvalidPublicKeyFile(fname.into()));
        }
        let key = pem.contents().try_into().map_err(|_| HubError::KeyVerify)?;
        let pubkey =
            ed25519_dalek::VerifyingKey::from_bytes(key).map_err(|_| HubError::KeyVerify)?;
        Ok(PublicKey { pubkey })
    }

    pub fn to_bytes(&self) -> [u8; 32] {
        self.pubkey.to_bytes()
    }

    pub fn write(&self, fname: &str) -> Result<()> {
        let pem = Pem::new(PUBLIC_KEY_TAG, self.to_bytes().to_vec());
        let buf = pem::encode(&pem);
        std::fs::write(fname, buf)?;
        Ok(())
    }

    pub fn verify(&self, msg: &[u8], sig: &Signature) -> Result<()> {
        self.pubkey
            .verify(msg, sig)
            .map_err(|_| HubError::SignatureError)?;
        Ok(())
    }

    pub fn ref_dalek(&self) -> &ed25519_dalek::VerifyingKey {
        &self.pubkey
    }

    pub fn from_hex(hexstring: &str) -> Result<PublicKey> {
        let pkbytes = hex::decode(hexstring).map_err(|_| HubError::KeyVerify)?;
        let arrbytes = pkbytes.try_into().map_err(|_| HubError::KeyVerify)?;
        let pk = PublicKey {
            pubkey: ed25519_dalek::VerifyingKey::from_bytes(&arrbytes)
                .map_err(|_| HubError::KeyVerify)?,
        };
        Ok(pk)
    }

    /// a pubkey from an ssh key (id_ed25519.pub) generated via
    ///  e.g. $ ssh-keygen -t ed25519 -C "sshkey@example.com" -f ./id_ed25519 -P ""
    /// ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIACGeGHWvt/E60k/FLuDsCkArLAIa4lvwk1wg3nJIGJl sshkey@example.com
    pub fn from_ssh(env_val: &str) -> Result<PublicKey> {
        let sshpubkey =
            ssh_key::PublicKey::from_openssh(env_val).map_err(|_| HubError::KeyVerify)?;
        let ekey = sshpubkey.key_data().ed25519().ok_or(HubError::KeyVerify)?;
        let pubkey = ed25519_dalek::VerifyingKey::from_bytes(ekey.as_ref())
            .map_err(|_| HubError::KeyVerify)?;
        Ok(PublicKey { pubkey })
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.pubkey.as_bytes())
    }
}

#[cfg(unix)]
fn set_perms_owner_rw(file: &mut std::fs::File) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut perms = file.metadata()?.permissions();
    perms.set_mode(0o500); // owner read write only
    Ok(())
}

#[cfg(not(unix))]
fn set_perms_owner_rw(_file: &mut std::fs::File) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod sshkeys {

    use super::Keypair;
    use super::PublicKey;

    #[test]
    fn workflow() {
        const PRIVFILE: &str = "tests/id_ed25519";
        const PUBFILE: &str = "tests/id_ed25519.pub";

        let buf = std::fs::read_to_string(PRIVFILE).expect("read in");
        let kp = Keypair::from_ssh(&buf).expect("keypair from_ssh failure");

        let buf = std::fs::read_to_string(PUBFILE).expect("read in");
        let pubkey = PublicKey::from_ssh(&buf).expect("reading ssh pub key file");

        let msg = b"123";
        let sig = kp.sign(msg).expect("sign failure");
        let verify = pubkey.verify(msg, &sig);
        assert!(verify.is_ok());
    }

    #[test]
    fn test_read_from_file() {
        const KEYFILE: &str = "tests/key_test.pem";

        let kp: Keypair = Keypair::new().expect("keypair creation error");
        kp.write_keypair(KEYFILE).expect("keypair write error");

        let _keypair = Keypair::read_from_file(KEYFILE).expect("key read error");
    }

    #[test]
    fn test_from_hex() {
        let kp: Keypair = Keypair::new().expect("keypair creation error");

        let pubhex = kp.public().to_hex();

        let _pubkey_from_hex = PublicKey::from_hex(&pubhex).expect("hex read error");
    }

    #[test]
    fn new_write_read_roundtrip() {
        let tmpdir = std::env::temp_dir();
        let kpfile = tmpdir.join("file.kp");
        let kp = Keypair::new().expect("keypair creation error");
        let kpfile_s = kpfile.display().to_string();
        let res = kp.write_keypair(&kpfile_s);
        assert!(res.is_ok(), "{res:?}");
        let res = Keypair::read_from_file(&kpfile_s);
        assert!(res.is_ok(), "{res:?}");
    }
}

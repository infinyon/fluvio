//! Key Mgmt module provides a thin abstraction over the public key
//! signing and validation ed25519-dalek crate
//! ed25519-dalek is a common crate between jwt token crates, package signing/verification
//! and ssh key representations.

use std::io::Write;
use std::os::unix::fs::PermissionsExt;

use ed25519_dalek::{Signer, Verifier};
use pem::Pem;

use crate::errors::HubUtilError;
use crate::errors::Result;

// keypair containing private and public keys
pub struct Keypair {
    kp: ed25519_dalek::Keypair,
}

impl Keypair {
    // failable clone
    pub fn clone_with_result(&self) -> Result<Self> {
        use ed25519_dalek::SecretKey;
        let secret = SecretKey::from_bytes(&self.kp.secret.to_bytes())
            .map_err(|_| HubUtilError::KeyVerify)?;
        let kp = ed25519_dalek::Keypair {
            secret,
            public: self.kp.public,
        };
        Ok(Keypair { kp })
    }
}

#[derive(Clone, Debug)]
pub struct PublicKey {
    pubkey: ed25519_dalek::PublicKey,
}

pub type Signature = ed25519_dalek::Signature;

impl Keypair {
    pub fn new() -> Result<Keypair> {
        use ed25519_dalek::{SecretKey, SECRET_KEY_LENGTH};
        use rand::RngCore;
        use rand::{thread_rng};

        // from jwt compact tests generate bytes
        // Since `ed25519_dalek` works with `rand` v0.7 rather than v0.8, we use this roundabout way
        // to generate a keypair.
        let mut secret = [0_u8; SECRET_KEY_LENGTH];
        thread_rng().fill_bytes(&mut secret);
        let secret = SecretKey::from_bytes(&secret)
            .map_err(|_| HubUtilError::General("Key generation error".into()))?;
        Ok(Keypair {
            kp: ed25519_dalek::Keypair {
                public: (&secret).into(),
                secret,
            },
        })
    }

    /// sign the message, generally files in the package
    pub fn sign(&self, buf: &[u8]) -> Result<Signature> {
        let sig = self
            .kp
            .try_sign(buf)
            .map_err(|_| HubUtilError::SignatureError)?;
        Ok(sig)
    }

    pub fn public(&self) -> PublicKey {
        PublicKey {
            pubkey: self.kp.public,
        }
    }

    /// read private key and derive public to populate keypair
    pub fn read_from_file(fname: &str) -> Result<Keypair> {
        let buf = std::fs::read(fname)?;
        let pem = pem::parse(&buf).map_err(|_| HubUtilError::InvalidKeyPairFile(fname.into()))?;
        if pem.tag != "PRIVATE KEY" {
            return Err(HubUtilError::InvalidKeyPairFile(fname.into()));
        }
        Keypair::from_secret_bytes(&pem.contents)
    }

    /// writes the private key from which the public is derivable on load
    pub fn write_keypair(&self, fname: &str) -> Result<()> {
        let pubpem = Pem {
            tag: "PRIVATE KEY".into(),
            contents: self.kp.secret.to_bytes().to_vec(),
        };
        let buf = pem::encode(&pubpem);
        let mut file = std::fs::File::create(fname)?;
        let mut perms = file.metadata()?.permissions();
        perms.set_mode(0o500); // owner read write only
        file.write_all(buf.as_bytes())?;
        Ok(())
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.kp.secret.as_bytes())
    }

    pub fn from_hex(hexstring: &str) -> Result<Keypair> {
        let pkbytes = hex::decode(hexstring).map_err(|_| HubUtilError::KeyVerify)?;
        Keypair::from_secret_bytes(&pkbytes)
    }

    /// a pubkey from an ssh private key (id_ed25519) generated via
    ///  e.g. $ ssh-keygen -t ed25519 -C "sshkey@example.com" -f ./id_ed25519 -P ""
    /// ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIACGeGHWvt/E60k/FLuDsCkArLAIa4lvwk1wg3nJIGJl sshkey@example.com
    pub fn from_ssh(env_val: &str) -> Result<Keypair> {
        let sshprivkey =
            ssh_key::PrivateKey::from_openssh(&env_val).expect("reading ssh pub key file");
        let keypair = sshprivkey
            .key_data()
            .ed25519()
            .ok_or(HubUtilError::KeyVerify)?;
        Keypair::from_secret_bytes(keypair.private.as_ref())
    }

    fn from_secret_bytes(sbytes: &[u8]) -> Result<Keypair> {
        let skey =
            ed25519_dalek::SecretKey::from_bytes(sbytes).map_err(|_| HubUtilError::KeyVerify)?;
        let pkey: ed25519_dalek::PublicKey = (&skey).into();
        let ekeypair = ed25519_dalek::Keypair {
            secret: skey,
            public: pkey,
        };
        Ok(Keypair { kp: ekeypair })
    }

    pub fn ref_dalek(&self) -> &ed25519_dalek::Keypair {
        &self.kp
    }
}

impl PublicKey {
    pub fn read_from_file(fname: &str) -> Result<PublicKey> {
        let buf = std::fs::read(fname)?;
        let pem = pem::parse(&buf).map_err(|_| HubUtilError::InvalidPublicKeyFile(fname.into()))?;
        if pem.tag != "PUBLIC KEY" {
            return Err(HubUtilError::InvalidPublicKeyFile(fname.into()));
        }
        let pubkey = ed25519_dalek::PublicKey::from_bytes(&pem.contents)
            .map_err(|_| HubUtilError::KeyVerify)?;
        Ok(PublicKey { pubkey })
    }

    pub fn to_bytes(&self) -> [u8; 32] {
        self.pubkey.to_bytes()
    }

    pub fn write(&self, fname: &str) -> Result<()> {
        let pubpem = Pem {
            tag: "PUBLIC KEY".into(),
            contents: self.to_bytes().to_vec(),
        };
        let buf = pem::encode(&pubpem);
        std::fs::write(fname, &buf)?;
        Ok(())
    }

    pub fn verify(&self, msg: &[u8], sig: &Signature) -> Result<()> {
        self.pubkey
            .verify(msg, sig)
            .map_err(|_| HubUtilError::SignatureError)?;
        Ok(())
    }

    pub fn ref_dalek(&self) -> &ed25519_dalek::PublicKey {
        &self.pubkey
    }

    pub fn from_hex(hexstring: &str) -> Result<PublicKey> {
        let pkbytes = hex::decode(hexstring).map_err(|_| HubUtilError::KeyVerify)?;
        let pk = PublicKey {
            pubkey: ed25519_dalek::PublicKey::from_bytes(&pkbytes)
                .map_err(|_| HubUtilError::KeyVerify)?,
        };
        Ok(pk)
    }

    /// a pubkey from an ssh key (id_ed25519.pub) generated via
    ///  e.g. $ ssh-keygen -t ed25519 -C "sshkey@example.com" -f ./id_ed25519 -P ""
    /// ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIACGeGHWvt/E60k/FLuDsCkArLAIa4lvwk1wg3nJIGJl sshkey@example.com
    pub fn from_ssh(env_val: &str) -> Result<PublicKey> {
        let sshpubkey =
            ssh_key::PublicKey::from_openssh(env_val).map_err(|_| HubUtilError::KeyVerify)?;
        let ekey = sshpubkey
            .key_data()
            .ed25519()
            .ok_or(HubUtilError::KeyVerify)?;
        let pubkey = ed25519_dalek::PublicKey::from_bytes(ekey.as_ref())
            .map_err(|_| HubUtilError::KeyVerify)?;
        Ok(PublicKey { pubkey })
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.pubkey.as_bytes())
    }
}

#[cfg(test)]
mod sshkeys {

    const PRIVFILE: &str = "tests/id_ed25519";
    const PUBFILE: &str = "tests/id_ed25519.pub";

    use super::Keypair;
    use super::PublicKey;

    #[test]
    fn workflow() {
        let buf = std::fs::read_to_string(PRIVFILE).expect("read in");
        let kp = Keypair::from_ssh(&buf).expect("keypair from_ssh failure");

        let buf = std::fs::read_to_string(PUBFILE).expect("read in");
        let pubkey = PublicKey::from_ssh(&buf).expect("reading ssh pub key file");
        dbg!(&pubkey);

        let msg = b"123";
        let sig = kp.sign(msg).expect("sign failure");
        let verify = pubkey.verify(msg, &sig);
        assert!(verify.is_ok());
    }
}

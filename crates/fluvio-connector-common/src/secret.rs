use std::{
    io::{BufReader, BufRead},
    path::{PathBuf, Path},
    fs::File,
};

use once_cell::sync::OnceCell;
use serde::{Serialize, Deserialize, de::Visitor, Deserializer};
use anyhow::{Result, anyhow};

static SECRET_STORE: OnceCell<Box<dyn SecretStore>> = OnceCell::new();

#[derive(PartialEq, Eq)]
pub struct SecretString {
    kind: SecretKind,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Secret {
    name: String,
}

#[derive(Default)]
pub struct EnvSecretStore;

#[derive(Debug)]
pub struct FileSecretStore {
    path: PathBuf,
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum SecretKind {
    String(String),
    Secret(Secret),
}

impl SecretString {
    pub fn resolve(&self) -> Result<String> {
        match &self.kind {
            SecretKind::String(s) => Ok(s.to_owned()),
            SecretKind::Secret(s) => default_secret_store()?.read(&s.name),
        }
    }

    pub fn resolve_from(&self, store: &dyn SecretStore) -> Result<String> {
        match &self.kind {
            SecretKind::String(s) => Ok(s.to_owned()),
            SecretKind::Secret(s) => store.read(&s.name),
        }
    }
}

impl<T: Into<String>> From<T> for SecretString {
    fn from(value: T) -> Self {
        let kind = SecretKind::String(value.into());
        Self { kind }
    }
}

impl From<Secret> for SecretString {
    fn from(value: Secret) -> Self {
        let kind = SecretKind::Secret(value);
        Self { kind }
    }
}

pub trait SecretStore: Send + Sync {
    fn read(&self, name: &str) -> Result<String>;
}

impl SecretStore for EnvSecretStore {
    fn read(&self, name: &str) -> Result<String> {
        std::env::var(name).map_err(|_| anyhow!("value not found for secret name {name}"))
    }
}

impl SecretStore for FileSecretStore {
    /// we intentionally read a file for every secret seek in order to minimize traces of sensitive
    /// data in the heap
    fn read(&self, name: &str) -> Result<String> {
        let file = File::open(&self.path)?;
        let buf_reader = BufReader::new(file);
        for line in buf_reader.lines() {
            if let Some((key, value)) = line?.split_once('=') {
                if key.trim().eq(name.trim()) {
                    return Ok(value.trim().to_owned());
                }
            }
        }
        anyhow::bail!("value not found for secret name {name}")
    }
}

impl<T: AsRef<Path>> From<T> for FileSecretStore {
    fn from(value: T) -> Self {
        Self {
            path: value.as_ref().to_owned(),
        }
    }
}

fn default_secret_store() -> Result<&'static (dyn SecretStore)> {
    SECRET_STORE
        .get()
        .map(AsRef::as_ref)
        .ok_or_else(|| anyhow!("global secret store is not set"))
}

pub fn set_default_secret_store(store: Box<dyn SecretStore>) -> Result<()> {
    SECRET_STORE
        .set(store)
        .map_err(|_| anyhow!("secret store is already set"))
}

struct SecretStringVisitor;
impl<'de> Visitor<'de> for SecretStringVisitor {
    type Value = SecretString;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("string or struct Secret")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(SecretString::from(v))
    }

    fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let kind: SecretKind =
            Deserialize::deserialize(serde::de::value::MapAccessDeserializer::new(map))?;
        Ok(SecretString { kind })
    }
}

impl<'de> Deserialize<'de> for SecretString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(SecretStringVisitor)
    }
}

impl Serialize for SecretString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match &self.kind {
            SecretKind::String(s) => s.serialize(serializer),
            other => other.serialize(serializer),
        }
    }
}

impl core::fmt::Debug for SecretString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut dbg_struct = f.debug_tuple("SecretString");
        match &self.kind {
            SecretKind::String(_) => dbg_struct.field(&"****"),
            SecretKind::Secret(secret) => dbg_struct.field(secret),
        };
        dbg_struct.finish()
    }
}

#[cfg(test)]
mod tests {

    use std::io::Write;

    use super::*;

    #[derive(Serialize, Deserialize, Debug)]
    struct TestCfg {
        some_secret: SecretString,
    }

    #[test]
    fn test_deser_from_raw_str() -> Result<()> {
        //given
        let input = r#"
        some_secret: secret_value
        "#;

        //when
        let parsed: TestCfg = serde_yaml::from_str(input)?;

        //then
        assert_eq!(parsed.some_secret.resolve()?, "secret_value");
        Ok(())
    }

    #[test]
    fn test_deser_from_struct() -> Result<()> {
        //given
        let input = r#"
        some_secret: 
         secret:
          name: secret_name
        "#;

        //when
        let parsed: TestCfg = serde_yaml::from_str(input)?;

        //then
        assert_eq!(
            parsed.some_secret,
            Secret {
                name: "secret_name".to_string()
            }
            .into()
        );
        Ok(())
    }

    #[test]
    fn test_ser_raw_str() -> Result<()> {
        //given
        let input = TestCfg {
            some_secret: "secret_value".into(),
        };

        //when
        let serialized = serde_yaml::to_string(&input)?;

        //then
        assert_eq!(serialized, "---\nsome_secret: secret_value\n");
        Ok(())
    }

    #[test]
    fn test_ser_struct() -> Result<()> {
        //given
        let input = TestCfg {
            some_secret: Secret {
                name: "secret_name".into(),
            }
            .into(),
        };

        //when
        let serialized = serde_yaml::to_string(&input)?;

        //then
        assert_eq!(
            serialized,
            "---\nsome_secret:\n  secret:\n    name: secret_name\n"
        );
        Ok(())
    }

    #[test]
    fn test_debug_raw_string() {
        //given
        let secret = SecretString::from("secret_value");

        //when
        let dbg = format!("{secret:?}");

        //then
        assert_eq!(dbg, "SecretString(\"****\")");
    }

    #[test]
    fn test_debug_struct() {
        //given
        let secret = SecretString::from(Secret {
            name: "secret_name".to_string(),
        });

        //when
        let dbg = format!("{secret:?}");

        //then
        assert_eq!(dbg, "SecretString(Secret { name: \"secret_name\" })");
    }

    #[test]
    fn test_resolve_when_store_not_set() {
        //given
        let secret = SecretString::from(Secret {
            name: "secret_name".to_string(),
        });

        //when
        let res = secret.resolve();

        //then
        assert!(res.is_err())
    }

    #[test]
    fn test_resolve_from_env() -> Result<()> {
        //given
        let secret_name = "test_resolve_from_env";
        let secret_value = "secret_value";
        let secret = SecretString::from(Secret {
            name: secret_name.to_string(),
        });
        let store = EnvSecretStore;

        //when
        std::env::set_var(secret_name, secret_value);
        let resolved = secret.resolve_from(&store)?;

        //then
        assert_eq!(resolved, secret_value);
        Ok(())
    }

    #[test]
    fn test_resolve_from_file() -> Result<()> {
        //given
        let mut file = tempfile::NamedTempFile::new()?;
        file.write_all(b"key1=value1\ntest_resolve_from_file=secret_value\nkey2=value2\n")?;
        let secret = SecretString::from(Secret {
            name: "test_resolve_from_file".to_string(),
        });
        let store = FileSecretStore::from(file.path());

        //when
        let resolved = secret.resolve_from(&store)?;

        //then
        assert_eq!(resolved, "secret_value");
        Ok(())
    }

    #[test]
    fn test_resolve_from_file_str_trim() -> Result<()> {
        //given
        let mut file = tempfile::NamedTempFile::new()?;
        file.write_all(b"key1=value1\n test_resolve_from_file = secret_value\nkey2=value2\n")?;
        let secret = SecretString::from(Secret {
            name: "test_resolve_from_file".to_string(),
        });
        let store = FileSecretStore::from(file.path());

        //when
        let resolved = secret.resolve_from(&store)?;

        //then
        assert_eq!(resolved, "secret_value");
        Ok(())
    }
}

#![allow(clippy::assign_op_pattern)]

use std::convert::Infallible;
use std::ops::Deref;
use std::str::FromStr;
use std::collections::BTreeMap;
use std::fmt;

use bytes::Buf;
use bytes::BufMut;

use fluvio_protocol::{Encoder, Decoder, Version};

#[derive(Encoder, Decoder, Default, Debug, Eq, PartialEq, Clone)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct ManagedConnectorSpec {
    pub name: String,

    pub version: ConnectorVersionInner,

    #[cfg_attr(feature = "use_serde", serde(rename = "type"))]
    pub type_: String, // syslog, github star, slack

    pub topic: String,

    pub parameters: BTreeMap<String, ManagedConnectorParameterValue>,

    pub secrets: BTreeMap<String, SecretString>,
}
#[derive(Debug, Eq, PartialEq, Clone)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase", untagged)
)]
pub enum ConnectorVersionInner {
    String(String),
    Option(Option<String>),
}

impl Default for ConnectorVersionInner {
    fn default() -> Self {
        Self::String(String::new())
    }
}
impl From<String> for ConnectorVersionInner {
    fn from(inner: String) -> Self {
        Self::String(inner)
    }
}
impl ToString for ConnectorVersionInner {
    fn to_string(&self) -> String {
        match self {
            ConnectorVersionInner::String(inner) => inner.to_string(),
            ConnectorVersionInner::Option(inner) => {
                inner.clone().unwrap_or_else(|| "latest".to_string())
            }
        }
    }
}

impl Decoder for ConnectorVersionInner {
    fn decode<T: Buf>(&mut self, src: &mut T, version: Version) -> Result<(), std::io::Error> {
        if version >= 9 {
            let mut new_string = String::new();
            new_string.decode(src, version)?;
            *self = ConnectorVersionInner::String(new_string);

            Ok(())
        } else {
            let mut inner: Option<String> = None;
            inner.decode(src, version)?;
            *self = ConnectorVersionInner::Option(inner);
            Ok(())
        }
    }
}
impl Encoder for ConnectorVersionInner {
    fn write_size(&self, version: Version) -> usize {
        if version >= 9 {
            let inner: String = self.clone().to_string();
            inner.write_size(version)
        } else {
            let inner: Option<String> = Some(self.clone().to_string());
            inner.write_size(version)
        }
    }

    /// encoding contents for buffer
    fn encode<T: BufMut>(&self, dest: &mut T, version: Version) -> Result<(), std::io::Error> {
        if version >= 9 {
            let inner: String = self.clone().to_string();
            inner.encode(dest, version)
        } else {
            let inner: Option<String> = Some(self.clone().to_string());
            inner.encode(dest, version)
        }
    }
}

#[derive(Encoder, Decoder, Debug, Eq, PartialEq, Clone)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize),
    serde(rename_all = "camelCase", untagged)
)]
pub enum ManagedConnectorParameterValueInner {
    Vec(Vec<String>),
    Map(BTreeMap<String, String>),
    String(String),
}

impl Default for ManagedConnectorParameterValueInner {
    fn default() -> Self {
        Self::Vec(Vec::new())
    }
}

#[cfg(feature = "use_serde")]
mod always_string_serialize {
    use super::ManagedConnectorParameterValueInner;
    use serde::{Deserializer, Deserialize};
    use serde::de::{self, Visitor, SeqAccess, MapAccess};
    use std::fmt;
    use std::collections::BTreeMap;
    struct ParameterValueVisitor;
    impl<'de> Deserialize<'de> for ManagedConnectorParameterValueInner {
        fn deserialize<D>(deserializer: D) -> Result<ManagedConnectorParameterValueInner, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_any(ParameterValueVisitor)
        }
    }

    impl<'de> Visitor<'de> for ParameterValueVisitor {
        type Value = ManagedConnectorParameterValueInner;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("string, map or sequence")
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            self.visit_str("null")
        }
        fn visit_unit<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            self.visit_str("null")
        }

        fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            self.visit_str(&v.to_string())
        }
        fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            self.visit_str(&v.to_string())
        }

        fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            self.visit_str(&v.to_string())
        }

        fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            self.visit_str(&v.to_string())
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(ManagedConnectorParameterValueInner::String(
                value.to_string(),
            ))
        }

        fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
        where
            M: MapAccess<'de>,
        {
            let mut inner = BTreeMap::new();
            while let Some((key, value)) = map.next_entry::<String, String>()? {
                inner.insert(key.clone(), value.clone());
            }

            Ok(ManagedConnectorParameterValueInner::Map(inner))
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut vec_inner = Vec::new();
            while let Some(param) = seq.next_element::<String>()? {
                vec_inner.push(param);
            }
            Ok(ManagedConnectorParameterValueInner::Vec(vec_inner))
        }
    }

    #[test]
    fn deserialize_test() {
        let yaml = r#"
name: kafka-out
parameters:
  param_1: "param_str"
  param_2:
   - item_1
   - item_2
   - 10
   - 10.0
   - true
   - On
   - Off
   - null
  param_3:
    arg1: val1
    arg2: 10
    arg3: -10
    arg4: false
    arg5: 1.0
    arg6: null
    arg7: On
    arg8: Off
  param_4: 10
  param_5: 10.0
  param_6: -10
  param_7: True
  param_8: 0xf1
  param_9: null
  param_10: 12.3015e+05
  param_11: [On, Off]
  param_12: true
secrets: {}
topic: poc1
type: kafka-sink
version: latest
"#;
        use super::ManagedConnectorSpec;
        let connector_spec: ManagedConnectorSpec =
            serde_yaml::from_str(yaml).expect("Failed to deserialize");
        println!("{:?}", connector_spec);
    }
}

#[derive(Default, Debug, Eq, PartialEq, Clone)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(transparent)
)]
pub struct ManagedConnectorParameterValue(pub ManagedConnectorParameterValueInner);

impl From<Vec<String>> for ManagedConnectorParameterValue {
    fn from(vec: Vec<String>) -> ManagedConnectorParameterValue {
        ManagedConnectorParameterValue(ManagedConnectorParameterValueInner::Vec(vec))
    }
}
impl From<BTreeMap<String, String>> for ManagedConnectorParameterValue {
    fn from(map: BTreeMap<String, String>) -> ManagedConnectorParameterValue {
        ManagedConnectorParameterValue(ManagedConnectorParameterValueInner::Map(map))
    }
}
impl From<String> for ManagedConnectorParameterValue {
    fn from(inner: String) -> ManagedConnectorParameterValue {
        ManagedConnectorParameterValue(ManagedConnectorParameterValueInner::String(inner))
    }
}

impl Decoder for ManagedConnectorParameterValue {
    fn decode<T: Buf>(&mut self, src: &mut T, version: Version) -> Result<(), std::io::Error> {
        if version >= 8 {
            self.0.decode(src, version)
        } else {
            let mut new_string = String::new();
            new_string.decode(src, version)?;
            self.0 = ManagedConnectorParameterValueInner::String(new_string);
            Ok(())
        }
    }
}
impl Encoder for ManagedConnectorParameterValue {
    fn write_size(&self, version: Version) -> usize {
        if version >= 8 {
            self.0.write_size(version)
        } else {
            match &self.0 {
                ManagedConnectorParameterValueInner::String(ref inner) => inner.write_size(version),
                _ => String::new().write_size(version),
            }
        }
    }

    /// encoding contents for buffer
    fn encode<T: BufMut>(&self, dest: &mut T, version: Version) -> Result<(), std::io::Error> {
        if version >= 8 {
            self.0.encode(dest, version)
        } else {
            match &self.0 {
                ManagedConnectorParameterValueInner::String(ref inner) => {
                    inner.encode(dest, version)
                }
                _ => String::new().encode(dest, version),
            }
        }
    }
}

#[derive(Encoder, Decoder, Default, Eq, PartialEq, Clone)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]

/// Wrapper for string that does not reveal its internal
/// content in its display and debug implementation
pub struct SecretString(String);

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[REDACTED]")
    }
}

impl fmt::Display for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

impl FromStr for SecretString {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.into()))
    }
}

impl From<String> for SecretString {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl Deref for SecretString {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

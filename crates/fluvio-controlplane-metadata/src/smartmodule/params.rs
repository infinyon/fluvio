use std::{
    collections::{BTreeMap},
};

use fluvio_protocol::{Encoder, Decoder};

#[derive(Debug, Default, Clone, PartialEq, Eq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SmartModuleParams(
    #[cfg_attr(feature = "use_serde", serde(default), serde(with = "map_init_params"))]
    BTreeMap<String, SmartModuleParam>,
);

impl SmartModuleParams {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn get_param(&self, name: &str) -> Option<&SmartModuleParam> {
        self.0.get(name)
    }

    pub fn insert_param(&mut self, name: String, param: SmartModuleParam) {
        self.0.insert(name, param);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Encoder, Default, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SmartModuleParam {
    pub description: Option<String>,
    #[cfg_attr(feature = "serde", serde(default))]
    pub optional: bool,
}

/// map parameters from list to map and vice versa
/// this is only used for k8
#[cfg(feature = "serde")]
mod map_init_params {
    use std::{collections::BTreeMap};

    use serde::{Serializer, Serialize, Deserializer, Deserialize};

    use super::SmartModuleParam;

    // convert btreemap into param of vec
    pub fn serialize<S>(
        data: &BTreeMap<String, SmartModuleParam>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let param_seq: Vec<K8Param> = data
            .iter()
            .map(|(k, v)| K8Param {
                name: k.clone(),
                param: v.clone(),
            })
            .collect();
        param_seq.serialize(serializer)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<BTreeMap<String, SmartModuleParam>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let param_list: Vec<K8Param> = Vec::deserialize(deserializer)?;
        let mut params = BTreeMap::new();
        for k8_param in param_list {
            params.insert(k8_param.name, k8_param.param);
        }
        Ok(params)
    }

    #[derive(Serialize, Deserialize, Clone)]
    struct K8Param {
        name: String,
        #[serde(flatten)]
        param: SmartModuleParam,
    }
}

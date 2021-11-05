//!
//! # SmartStream Status
//!
use std::fmt;

use dataplane::core::{Encoder, Decoder};

// -----------------------------------
// Data Structures
// -----------------------------------

#[derive(Default, Decoder, Encoder, Debug, Clone, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct SmartStreamStatus {
    pub resolution: SmartStreamResolution,
}

impl fmt::Display for SmartStreamStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SmartStreamStatus")
    }
}

#[derive(Decoder, Encoder, Debug, Clone, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SmartStreamResolution {
    Init,
    InvalidConfig(String),
    Provisioned,
}

impl Default for SmartStreamResolution {
    fn default() -> Self {
        SmartStreamResolution::Init
    }
}

mod states {

    use fluvio_stream_model::core::MetadataItem;
    use fluvio_stream_model::store::LocalStore;

    use crate::smartstream::{SmartStreamSpec, SmartStreamValidationInput};
    use crate::smartmodule::SmartModuleSpec;

    use super::*;

    impl SmartStreamResolution {
        /// for this resolution, compute next state based on smartmodules
        pub async fn next<'a, C>(
            &'a self,
            spec: &SmartStreamSpec,
            objects: &SmartStreamValidationInput<'a, C>,
        ) -> Option<Self>
        where
            C: MetadataItem,
        {
            match self {
                Self::Init | Self::InvalidConfig(_) => match spec.validate(&objects).await {
                    Ok(()) => Some(Self::Provisioned),
                    Err(e) => Some(Self::InvalidConfig(e.to_string())),
                },
                Self::Provisioned => None,
            }
        }
    }

    /// ensure all modules are provisioned
    fn validate_modules<C>(modules: &LocalStore<SmartModuleSpec, C>) -> SmartStreamResolution
    where
        C: MetadataItem,
    {
        SmartStreamResolution::Provisioned
    }
}

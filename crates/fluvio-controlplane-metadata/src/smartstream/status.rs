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

    use tracing::trace;

    use fluvio_stream_model::core::MetadataItem;

    use crate::smartstream::{SmartStreamSpec, SmartStreamValidationInput};

    use super::*;

    impl SmartStreamResolution {
        /// for this resolution, compute next state based on smartmodules
        pub async fn next<'a, C>(
            &'a self,
            spec: &SmartStreamSpec,
            objects: &SmartStreamValidationInput<'a, C>,
            force: bool,
        ) -> Option<Self>
        where
            C: MetadataItem,
        {
            match self {
                Self::Init | Self::InvalidConfig(_) => {
                    trace!("init or invalid, performing validation");
                    match spec.validate(&objects).await {
                        Ok(()) => Some(Self::Provisioned),
                        Err(e) => Some(Self::InvalidConfig(e.to_string())),
                    }
                }
                Self::Provisioned => {
                    if force {
                        trace!("revalidating");
                        match spec.validate(&objects).await {
                            Ok(()) => None, // it is already validated
                            Err(e) => Some(Self::InvalidConfig(e.to_string())),
                        }
                    } else {
                        None
                    }
                }
            }
        }
    }
}

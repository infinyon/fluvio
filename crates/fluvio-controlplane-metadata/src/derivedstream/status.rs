//!
//! # DerivedStream Status
//!
use std::fmt;

use fluvio_protocol::{Encoder, Decoder};

// -----------------------------------
// Data Structures
// -----------------------------------

#[derive(Default, Decoder, Encoder, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct DerivedStreamStatus {
    pub resolution: DerivedStreamResolution,
}

impl fmt::Display for DerivedStreamStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#?}", self.resolution)
    }
}

impl DerivedStreamStatus {
    // whether this is valid (deployable)
    pub fn is_deployable(&self) -> bool {
        matches!(self.resolution, DerivedStreamResolution::Provisioned)
    }
}

#[derive(Decoder, Encoder, Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum DerivedStreamResolution {
    Init,
    InvalidConfig(String),
    Provisioned,
}

impl Default for DerivedStreamResolution {
    fn default() -> Self {
        DerivedStreamResolution::Init
    }
}

mod states {

    use tracing::{instrument, trace};

    use fluvio_stream_model::core::MetadataItem;

    use crate::derivedstream::{DerivedStreamSpec, DerivedStreamValidationInput};

    use super::*;

    impl DerivedStreamResolution {
        /// for this resolution, compute next state based on smartmodules
        #[instrument(skip(spec, objects))]
        pub async fn next<'a, C>(
            &'a self,
            spec: &DerivedStreamSpec,
            objects: &DerivedStreamValidationInput<'a, C>,
            force: bool,
        ) -> Option<Self>
        where
            C: MetadataItem,
        {
            match self {
                Self::Init => {
                    trace!("init or invalid, performing validation");
                    match spec.validate(objects).await {
                        Ok(()) => Some(Self::Provisioned),
                        Err(e) => Some(Self::InvalidConfig(e.to_string())),
                    }
                }
                Self::InvalidConfig(old_error) => {
                    if force {
                        trace!("revalidating invalid");
                        match spec.validate(objects).await {
                            Ok(()) => Some(Self::Provisioned),
                            Err(e) => {
                                trace!("invalid: {:#?}", e);
                                let new_error = e.to_string();
                                if old_error != &new_error {
                                    Some(Self::InvalidConfig(e.to_string()))
                                } else {
                                    trace!("same error as before");
                                    None
                                }
                            }
                        }
                    } else {
                        trace!("ignoring");
                        None
                    }
                }
                Self::Provisioned => {
                    if force {
                        trace!("revalidating provisoned");
                        match spec.validate(objects).await {
                            Ok(()) => None, // it is already validated
                            Err(e) => Some(Self::InvalidConfig(e.to_string())),
                        }
                    } else {
                        trace!("ignoring");
                        None
                    }
                }
            }
        }
    }
}

use std::convert::TryFrom;
use std::fmt::Debug;

use anyhow::{Result, Ok};
use fluvio_smartmodule::dataplane::smartmodule::{
    SmartModuleInitInput, SmartModuleInitOutput, SmartModuleInitErrorStatus,
};

use crate::engine::common::{WasmFn, WasmInstance};

use super::instance::SmartModuleInstanceContext;

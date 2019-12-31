//!
//! # Send Update Replica Leader Request Handlers
//!
//! Establishes connetion with Spus and sends Update Replica Leader Request
//!
use std::sync::Arc;

use kf_protocol::api::RequestMessage;

use internal_api::messages::{ReplicaMsg, ReplicaMsgs};
use internal_api::UpdateReplicaRequest;

use error::ServerError;
use log::{debug, error, trace};
use log::warn;
use utils::actions::Actions;

use crate::core::common::spu_notify_by_id::SpuNotifyById;
use crate::core::spus::{Spu, Spus};
 use crate::conn_manager::SpuConnections;
use crate::hc_manager::HcAction;


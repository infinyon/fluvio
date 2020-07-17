//!
//! # SC Spu Metadata
//!
//! Spu metadata information cached locally.
//!
use std::sync::Arc;

use log::{debug, trace};
use flv_types::log_on_err;
use flv_metadata::store::actions::*;

use crate::spu::*;
use crate::ScServerError;
use crate::stores::spu::*;
use crate::metadata::*;

use super::*;

/// SpuReducer is responsible for updating state for SPU
#[derive(Debug)]
pub struct SpuReducer(Arc<SpuAdminStore>);

impl SpuReducer {
    pub fn new<A>(store: A) -> Self
    where
        A: Into<Arc<SpuAdminStore>>,
    {
        Self(store.into())
    }

    pub async fn process_requests(
        &self,
        change_request: SpuChangeRequest,
    ) -> Result<SpuActions, ScServerError> {
        debug!("processing requests: {}", change_request);
        let mut actions = SpuActions::default();

        match change_request {
            SpuChangeRequest::SpuLS(ls_requests) => {
                for local_change in ls_requests.into_iter() {
                    match local_change {
                        LSChange::Add(spu) => {
                            log_on_err!(self.add_spu_action_handler(spu, &mut actions));
                        }

                        LSChange::Mod(new_spu, local_spu) => {
                            log_on_err!(self.mod_spu_action_handler(
                                new_spu,
                                local_spu,
                                &mut actions
                            ));
                        }

                        LSChange::Delete(spu) => {
                            log_on_err!(self.del_spu_action_handler(spu, &mut actions));
                        }
                    }
                }
            }

            SpuChangeRequest::Conn(conn_request) => {
                self.conn_status_update(conn_request, &mut actions).await;
            }
        }

        Ok(actions)
    }

    ///
    /// Handle when new SPU events from World KV
    ///
    fn add_spu_action_handler(
        &self,
        mut spu: SpuAdminMd,
        actions: &mut SpuActions,
    ) -> Result<(), ScServerError> {
        debug!("AddSpu({})", spu.key());
        trace!("add spu action handler {:#?}", spu);
        /*
        actions
            .conns
            .push(ConnectionRequest::Spu(SpuSpecChange::Add(
                spu.spec().clone(),
            )));
        */
        // always set to offline status
        if !spu.status.is_offline() {
            spu.status.set_offline();
            actions.spus.push(WSAction::UpdateStatus(spu));
        }

        Ok(())
    }

    ///
    /// Modify SPU Action handler
    ///
    /// # Remarks
    /// Action handler performs the following operations:
    /// * if spec changed
    ///     * update spec in local cache
    /// * if status changed,
    ///     * update status in local cache
    ///     * ask Topic to generate replica maps for topics waiting for additional SPUs
    ///     * notify Healthcheck module
    /// * if spu spec or status changed
    ///     *  update SPUs in cluster
    ///
    fn mod_spu_action_handler(
        &self,
        new_spu: SpuAdminMd,
        old_spu: SpuAdminMd,
        actions: &mut SpuActions,
    ) -> Result<(), ScServerError> {
        let spu_id = new_spu.spec.id;
        debug!("Update SPU({})", new_spu.key());
        trace!("Update SPU: new {:#?} old: {:#?}", new_spu, old_spu);

        // spec changed
        /*
        if new_spu.spec != old_spu.spec {
          
            actions
                .conns
                .push(ConnectionRequest::Spu(SpuSpecChange::Mod(
                    new_spu.spec,
                    old_spu.spec,
                )));
        }
        */
        /*
        // status changed
        if new_spu.status != old_spu.status {
            // if spu comes online
            //  * send SPU a full update
            //  * ask topic to generate replica map for pending topics

            if old_spu.status.is_offline() && new_spu.status.is_online() {
                actions.conns.push(ConnectionRequest::RefreshSpu(spu_id));
            }
        }
        */
        Ok(())
    }

    ///
    /// Delete Spu Action handler
    ///
    /// # Remarks
    /// Action handler performs the following operations:
    /// * delete SPU from local cache
    /// * notify Healthcheck module
    /// *  update SPUs in cluster
    ///
    fn del_spu_action_handler(
        &self,
        spu: SpuAdminMd,
        actions: &mut SpuActions,
    ) -> Result<(), ScServerError> {
        let _spu_id = spu.id();

        debug!("deleting spu: {}", spu.key());
        trace!("delete spu {:#?}", spu);

        /*
        actions
            .conns
            .push(ConnectionRequest::Spu(SpuSpecChange::Remove(spu.spec)));
        */
        Ok(())
    }

    
    /// notification from Connection Manager indicating connection status changed
    /// this will generate KV action
    async fn conn_status_update(&self, status: SpuConnectionStatusChange, actions: &mut SpuActions) {
        debug!("processing conn request: {}", status);
        let spu_id = status.spu_id();

        if let Some(spu) = self.0.get_by_id(spu_id).await {
            let mut spu_kv = spu.clone();
            match status {
                SpuConnectionStatusChange::Off(_) => spu_kv.status.set_offline(),
                SpuConnectionStatusChange::On(_) => spu_kv.status.set_online(),
            }

            actions.spus.push(WSAction::UpdateStatus(spu_kv));
        }
    }
    
}

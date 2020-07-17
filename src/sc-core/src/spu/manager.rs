//!
//! # Connection Manager (ConnManager)
//!
//! ConnManager keeps communication sockets between Streaming Coordinator (SC) and Streaming Processing
//! Units (SPUs) persistent. The manager keeps a map of SPU names with their associated socket handles.
//!
//! # ConnManager Actions
//!
//! SC notifies the ConnManager when a new SPU is joins or leaves the system:
//!     * ConnAction::AddSpu(SpuId, ServerAddress)      - SPU joins the system
//!     * ConnAction::UpdateSpu(SpuId, ServerAddress)   - SPU parameters are changed
//!     * ConnAction::RemoveSpu(SpuId)                  - SPU leaves the system
//!
//! Connections will be lazily handled. They are looked-up when a connection is requested. When SPU
//! parameters change, the connection is marked as stale and a new connection is generated.

use std::sync::Arc;

use log::debug;
use log::trace;
use log::error;
use log::warn;
use chashmap::WriteGuard;
use async_channel::Receiver as BoundedReceiver;


use flv_metadata::spu::SpuSpec;
use flv_metadata::partition::*;
use kf_socket::*;
use flv_types::SpuId;
use flv_types::log_on_err;
use flv_metadata::message::*;
use internal_api::UpdateSpuRequest;
use internal_api::UpdateReplicaRequest;
use internal_api::UpdateAllRequest;
use flv_future_aio::task::spawn;
use kf_protocol::api::*;

use crate::stores::partition::*;
use crate::stores::spu::*;
use crate::core::SharedContext;
use crate::ScServerError;

use super::*;



/// Discovered Connection Parameter such as source IP address and port
#[derive(Debug, Clone)]
pub struct ConnParams {}

impl ConnParams {
    pub fn new() -> Self {
        Self {}
    }
}

pub type SharedConnManager = Arc<ConnManager>;

/// Connection Manager handles actual connection to SPU
/// It is responsible for keeping track of connection stat, param and sinks.Actions
/// When it detects changes in connection (status), it publish them to senders
/// Unlikely controller, it doesn't have own independent task lifecyle (maybe should?)
#[derive(Debug)]
pub struct ConnManager {
    spu_store: Arc<SpuAdminStore>,
    partition_store: Arc<PartitionAdminStore>,
    receiver: BoundedReceiver<ConnRequests>,
    sinks: SinkPool<SpuId>,
}



impl ConnManager {
    pub fn new_with_local_stores(
        context: SharedContext,
        receiver: BoundedReceiver<ConnRequests>
    ) -> Self {
        Self::new(
            context.spus().clone(),
            context.partitions().clone(),
            receiver
        )
    }

    /// internal connection manager constructor
    pub fn new(
        spu_store: Arc<SpuAdminStore>, 
        partition_store: Arc<PartitionAdminStore>,
        receiver: BoundedReceiver<ConnRequests>
    ) -> Self {
        Self {
            spu_store,
            partition_store,
            sinks: SinkPool::new(),
            receiver
        }
    }

    /// start request loop
    pub fn run(self) {
        spawn(async move {
            self.dispatch_loop().await;
        });
    }

    async fn dispatch_loop(mut self) {

        debug!("starting request dispatch loop");

        loop {
            match self.receiver.recv().await {
                Ok(requests) => {
                    self.process_requests(requests).await;
                },
                Err(err) => {
                    panic!("error receiving requesgt: {}",err);
                }
            }
        }
    }

    /// Process connection requests
    /// Requests are usually send as result of action by other controller
    async fn process_requests(&self, requests: Vec<ConnectionRequest>) {
        
        debug!("processing requests: {} from controllers",requests.len());
        trace!("processing connection request: {:?}", requests);

        for request in requests.into_iter() {
            match request {
                ConnectionRequest::Spu(spec_changes) => {
                    match spec_changes {
                        SpuSpecChange::Add(new_spu) => {
                            self.add_spu(new_spu).await;
                        }
                        SpuSpecChange::Mod(new_spu, old_spu) => {
                            self.update_spu(new_spu, old_spu).await;
                        }
                        SpuSpecChange::Remove(spu) => {
                            self.remove_spu(spu).await;
                        }
                    }
                },
                ConnectionRequest::RefreshSpu(spu_id) => {
                    log_on_err!(self.refresh_spu(spu_id).await);
                }
                ConnectionRequest::Partition(partition_req) => {
                    match partition_req.clone() {
                        PartitionSpecChange::Add(key, spec) => {
                            self.refresh_partition(key, spec).await;
                        }
                        PartitionSpecChange::Mod(key, new_spec, _) => {
                            // for now, only send new
                            self.refresh_partition(key, new_spec).await;
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    /// SPU is valid if we have registered SPU in the store and if spu is offline
    async fn validate_spu(&self, spu_id: SpuId) -> bool {
        self.spu_store.validate_spu_for_registered(spu_id).await
    }

    /// Register new sink
    /// true if successfully register
    async fn register_sink(&self, spu_id: SpuId, sink: KfSink, _param: ConnParams) {
        self.sinks.insert_sink(spu_id.clone(), sink);
    }

    /// De-register sink.  This happens when connection when down
    async fn clear_sink(&self, spu_id: &SpuId) {
        self.sinks.clear_sink(spu_id);
        debug!("removing socket sink for spu: {}", spu_id);
    }

    /// return current sinks
    fn sinks(&self) -> &SinkPool<SpuId> {
        &self.sinks
    }

    

    /// synchronize spu spec with our connection
    /// if there exists spu connection, we need to drop it.
    fn inner_add_spu(&self, spu: &SpuSpec) {
        
        // there should not be existing entry, if so something is wrong
        if let Some(conn) = self.sinks.get_sink(&spu.id) {
            drop(conn);
            self.sinks.clear_sink(&spu.id);
            warn!(
                "unexpected socket entry found for Spu({}). clearing ",
                spu.id
            );
        }
    }

    /// add spu,
    async fn add_spu(&self, spu: SpuSpec) {
        
        debug!("Conn Mgr: processing New SPU: {}", spu.id);
        self.inner_add_spu(&spu);

        // send new SPU spec to all SPUS
        let spu_msg = vec![SpuMsg::update(spu.into())];
        self.send_msg_to_all_live_spus(&spu_msg).await;
    }

    /// update spu connection, we do similar thing as add.
    async fn update_spu(&self, new_spu: SpuSpec, old_spu: SpuSpec) {
        debug!("updating new spu: {}, old spu: {}", new_spu.id, old_spu.id);

        self.inner_remove_spu(&old_spu);
        self.inner_add_spu(&new_spu);

        let spu_msg = vec![SpuMsg::update(old_spu.into())];
        self.send_msg_to_all_live_spus(&spu_msg).await;
       // self.send_client_notification(ClientNotification::SPU(UpdateSpuResponse::new(spu_msg)));
    }

    /// remove spu connection parameters & socket.
    async fn remove_spu(&self, old_spu: SpuSpec) {
        debug!("remove Spu({}) from ConnMgr", old_spu.id);
        self.sinks.clear_sink(&old_spu.id);
       

        let spu_msg = vec![SpuMsg::delete(old_spu.into())];
        self.send_msg_to_all_live_spus(&spu_msg).await;
       // self.send_client_notification(ClientNotification::SPU(UpdateSpuResponse::new(spu_msg)));
    }

    /// remove spu connection parameters & socket.
    fn inner_remove_spu(&self, old_spu: &SpuSpec) {
        debug!("remove Spu({}) from ConnMgr", old_spu.id);
        self.sinks.clear_sink(&old_spu.id);
      
    }

    // -----------------------------------
    // Get Connection & Update status
    // -----------------------------------

    /// grab connection socket and increment counters
    pub fn get_mut_connection(&self, spu_id: SpuId) -> Option<WriteGuard<SpuId, KfSink>> {
        self.sinks.get_sink(&spu_id)
    }

  
    /// Update Partition information to all SPUs in the spec
    async fn refresh_partition(&self, key: ReplicaKey, spec: PartitionSpec) {
        // generate replica
        let mut replica_msgs = ReplicaMsgs::default();
        replica_msgs.push(ReplicaMsg::update(Replica::new(
            key,
            spec.leader,
            spec.replicas.clone(),
        )));

        let request = UpdateReplicaRequest::encode_request(replica_msgs.clone());
        let mut message = RequestMessage::new_request(request);
        message.get_mut_header().set_client_id("controller");

        for spu in spec.replicas {
            debug!(
                "sending replica: {} to spu: {}",
                message.request.decode_request(),
                spu
            );
            match self.send_msg(spu, &message).await {
                Ok(status) => {
                    if !status {
                        trace!(
                            "unable to send partition: {} to offline spu: {}",
                            spec.leader,
                            spu
                        );
                    }
                }
                Err(err) => warn!(
                    "error {} sending partition: {} to spu: {}",
                    err, spec.leader, spu
                ),
            }
        }

       // self.send_client_notification(ClientNotification::Replica(UpdateReplicaResponse::new(replica_msgs)));

    }

    /// looks-up metadata and sends all SPUs and Replicas leaders associated with the SPU.
    async fn refresh_spu(&self, spu_id: i32) -> Result<(), ScServerError> {
        debug!("Send SPU metadata({})", spu_id);

        if let Some(spu) = self.spu_store.get_by_id(spu_id).await {
            self.send_update_all_to_spu(&spu).await?;
        } else {
            return Err(ScServerError::UnknownSpu(spu_id));
        }

        Ok(())
    }

    /// send all changes to specific SPU
    async fn send_update_all_to_spu(&self, spu: &SpuAdminMd) -> Result<(), ScServerError> {
        let spu_specs = self.spu_store.clone_specs().await;
        let replicas = self.partition_store.replica_for_spu(spu.id()).await;
        let request = UpdateAllRequest::new(spu_specs, replicas);

        debug!(
            "SEND SPU Metadata: >> ({}): BulkUpdate({} spu-msgs, {} replica-msgs)",
            spu.id(),
            request.spus.len(),
            request.replicas.len(),
        );
        trace!("{:#?}", request);

        let mut message = RequestMessage::new_request(request);
        message.get_mut_header().set_client_id("controller");

        self.send_msg(spu.id(), &message).await?;

        Ok(())
    }

    /// send messages to all live SPU
    async fn send_msg_to_all_live_spus(&self, msgs: &Vec<SpuMsg>) {
        let online_spus = self.spu_store.online_spus().await;
        debug!(
            "trying to send SPU spec to active Spus: {}",
            online_spus.len()
        );
        for live_spu in online_spus {
            if let Err(err) = self
                .send_update_spu_msg_request(&live_spu, msgs.clone())
                .await
            {
                error!("error sending msg {}", err);
            }
        }
    }

    /// Send Update SPU message Request to an Spu
    async fn send_update_spu_msg_request<'a>(
        &'a self,
        spu: &'a SpuAdminMd,
        spu_msgs: Vec<SpuMsg>,
    ) -> Result<(), ScServerError> {
        trace!("{:#?}", spu_msgs);

        let request = UpdateSpuRequest::new(spu_msgs);

        let mut message = RequestMessage::new_request(request);
        message.get_mut_header().set_client_id("controller");

        self.send_msg(spu.id(), &message).await?;

        Ok(())
    }

    /// send request message to specific spu
    /// this is a one way 
    async fn send_msg<'a, R>(
        &'a self,
        spu_id: SpuId,
        req_msg: &'a RequestMessage<R>,
    ) -> Result<bool, ScServerError>
    where
        R: Request + Send + Sync + 'static,
    {
        if let Some(mut spu_conn) = self.get_mut_connection(spu_id) {
            // send message & evaluate response

            trace!("spu client: sending msg: {:#?}", req_msg);
            match spu_conn.send_request(&req_msg).await {
                Ok(_) => {
                    trace!("spu client send successfully");
                    Ok(true)
                }
                Err(err) => {
                    error!("spu client send failed");
                    Err(ScServerError::SpuCommuncationError(spu_id, err))
                }
            }
        } else {
            Ok(false)
        }
    }

    
}

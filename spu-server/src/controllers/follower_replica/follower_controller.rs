
use std::time::Duration;
use std::net::SocketAddr;
use std::convert::TryInto;

use log::trace;
use log::error;
use log::debug;


use futures::channel::mpsc::Receiver;
use futures::select;
use futures::StreamExt;
use futures::FutureExt;

use future_helper::spawn;
use future_helper::sleep;
use kf_socket::KfSocket;
use kf_socket::KfSink;
use kf_socket::KfSocketError;
use kf_protocol::api::RequestMessage;
use internal_api::messages::Replica;
use types::SpuId;
use types::log_on_err;
use storage::FileReplica;
use metadata::spu::SpuSpec;


use crate::controllers::leader_replica::UpdateOffsetRequest;
use crate::services::internal::FetchStreamRequest;
use crate::core::spus::SharedSpuLocalStore;
use crate::core::SharedSpuConfig;

use super::FollowerReplicaControllerCommand;
use super::FollowerReplicaState;
use super::KfFollowerPeerApiEnum;
use super::DefaultSyncRequest;
use super::FollowerPeerRequest;
use super::SharedFollowersState;

/// time to resync follower offsets to leader
const LEADER_RECONCILIATION_INTERVAL_SEC: u64 = 60; // 1 min

/// Controller for managing follower replicas
/// There is a controller for follower groups (group by leader SPU)
pub struct ReplicaFollowerController<S> {
    leader_id: SpuId,
    spu_localstore: SharedSpuLocalStore,
    followers_state: SharedFollowersState<S>,
    receiver: Receiver<FollowerReplicaControllerCommand>,
    config: SharedSpuConfig
}

impl <S>ReplicaFollowerController<S> {

    
    pub fn new(
        leader_id: SpuId, 
        receiver: Receiver<FollowerReplicaControllerCommand>,
        spu_localstore: SharedSpuLocalStore,
        followers_state: SharedFollowersState<S>,
        config: SharedSpuConfig
    ) -> Self {
        Self {
            leader_id,
            spu_localstore,
            receiver,
            followers_state,
            config
        }
    }
}

impl ReplicaFollowerController<FileReplica> {
    
    pub fn run(self) {

        spawn(self.dispatch_loop());
    }

    async fn dispatch_loop(mut self)  {

        debug!("starting follower replica controller for leader spu: {}",self.leader_id);
        loop {

            if let Some(socket) = self.create_socket_to_leader().await {

                // send initial fetch stream request
                debug!("established connection to leader: {}",self.leader_id);
                match self.stream_loop(socket).await {
                    Ok(terminate_flag) => {
                        if terminate_flag {
                            trace!("end command has received, terminating connection to leader: {}",self.leader_id);
                            break;
                        }
                    },
                    Err(err) => error!("connection error, connecting to leader: {} err: {:#?}",self.leader_id,err)
                }

                debug!("lost connection to leader: {}, sleeping 5 seconds and will retry it",self.leader_id);
                // 5 seconds is heuratic value, may change in the future or could be dynamic
                // depends on backoff algorithm
                sleep(Duration::from_secs(5)).await;

            } else {
                debug!("TODO: describe more where this can happen");
                break;
            }
        }
        debug!("shutting down follower controller: {}",self.leader_id);

    }

    async fn stream_loop(&mut self,mut socket: KfSocket) -> Result<bool,KfSocketError> {

        self.send_fetch_stream_request(&mut socket).await?;
        let (mut sink,mut stream) = socket.split();
        let mut api_stream = stream.api_stream::<FollowerPeerRequest,KfFollowerPeerApiEnum>();

        // sync offsets
        self.sync_all_offsets_to_leader(&mut sink).await;

        loop {

            log::trace!("waiting for Peer Request from leader: {}",self.leader_id);

            select! {
                _ = (sleep(Duration::from_secs(LEADER_RECONCILIATION_INTERVAL_SEC))).fuse() => {
                    debug!("timer fired - kickoff sync offsets to leader {}",self.leader_id);
                    self.sync_all_offsets_to_leader(&mut sink).await;
                },

                cmd_msg = self.receiver.next() => {
                    if let Some(cmd) = cmd_msg {
                        match cmd {
                            FollowerReplicaControllerCommand::AddReplica(replica) => {
                                debug!("leader: {}, adding replica: {}",self.leader_id,replica);
                                self.update_replica(replica).await;
                                self.sync_all_offsets_to_leader(&mut sink).await;
                            },
                            FollowerReplicaControllerCommand::UpdateReplica(replica) => {
                                self.update_replica(replica).await;
                                self.sync_all_offsets_to_leader(&mut sink).await;
                            }
                        }
                    } else {
                        debug!("mailbox to this controller: {} has been closed, shutting controller down",self.leader_id);
                        return Ok(true)
                    }
                },
                api_msg = api_stream.next().fuse() => {
                    
                    if let Some(req_msg_res) = api_msg {
                        match req_msg_res {
                            Ok(req_msg) => {
                                 match req_msg {
                                    FollowerPeerRequest::SyncRecords(sync_request) => self.write_to_follower_replica(&mut sink,sync_request.request).await,
                                 }

                            },
                            Err(err) => {
                                 log::trace!("error decoding request: {}, terminating connection",err);
                                 return Ok(false)
                            }
                        }
                      
                    } else {
                        trace!("leader socket has terminated");
                        return Ok(false);

                    }
                }
               
            }
        }
    }


    /// get available spu, this is case where follower request is received before rest of spu arrives from SC.
    /// TODO: remove wait call
    async fn get_spu(&self) -> SpuSpec {

        loop {
            if let Some(spu) =  self.spu_localstore.spec(&self.leader_id){
                return spu
            }

            trace!("leader spu spec: {} is not available, waiting 1 second",self.leader_id);
            sleep(Duration::from_millis(1000)).await;
            trace!("awake from sleep, checking spus: {}",self.leader_id);
        }
    }


    async fn write_to_follower_replica(&self,sink: &mut KfSink,req: DefaultSyncRequest) {

        debug!("handling sync request from leader: {}, req {}",self.leader_id,req);

        let offsets = self.followers_state.send_records(req).await;
        self.sync_offsets_to_leader(sink,offsets).await;

    }
    

    /// connect to leader, if can't connect try until we succeed
    /// or if we received termination message
    async fn create_socket_to_leader(&mut self) -> Option<KfSocket> {
 
        let leader_spu = self.get_spu().await;
        debug!("trying to resolve leader: {} addr: {}",leader_spu.id,leader_spu.private_endpoint.host);
        let addr: SocketAddr = leader_spu.private_server_address().try_into().expect("addr should succeed");
        debug!("resolved leader: {} addr: {}",leader_spu.id,addr);
        loop {

            trace!("trying to create socket to leader: {}",self.leader_id);
            let connect_future =  KfSocket::fusable_connect(&addr);
         
            select! {
                msg = self.receiver.next() => {
                    if let Some(cmd) = msg {
                        match cmd {
                            FollowerReplicaControllerCommand::AddReplica(replica) => self.update_replica(replica).await,
                            FollowerReplicaControllerCommand::UpdateReplica(replica) => self.update_replica(replica).await
                        }
                    } else {
                        error!("mailbox seems terminated, we should termina also");
                        return None
                    }
                },
                socket_res = connect_future.fuse() => {
                    match socket_res {
                        Ok(socket) => {
                            trace!("connected to leader: {}",self.leader_id);
                            return Some(socket)
                        }
                        Err(err) => error!("error connecting to leader: {}",err)
                    }

                    trace!("sleeping 5 seconds to connect to leader: {}",self.leader_id);
                    sleep(Duration::from_secs(5)).await;
                }
               
            }

           
            
        }   
    }


    /// send request to establish peer to peer communication to leader
    async fn send_fetch_stream_request(&self, socket: &mut KfSocket) -> Result<(),KfSocketError>{

        let local_spu_id = self.config.id();
        trace!("sending fetch stream for leader: {} for follower: {}",self.leader_id,local_spu_id);
        let mut fetch_request = FetchStreamRequest::default();
        fetch_request.spu_id = local_spu_id;
        let mut message = RequestMessage::new_request(fetch_request);
        message
            .get_mut_header()
            .set_client_id(format!("peer spu: {}",local_spu_id));

        let response = socket.send(&message).await?;
        trace!("fetch stream response: {:#?}",response);
        debug!("established peer to peer channel to leader: {} from follower: {}",self.leader_id,local_spu_id);
        Ok(())
    }

    /// create new replica if doesn't exist yet
    async fn update_replica(&self, replica_msg: Replica) {

        debug!("received update replica {} from leader: {}",replica_msg,self.leader_id);

        let replica_key = replica_msg.id.clone();
        if self.followers_state.has_replica(&replica_key) {
            debug!("has already follower replica: {}, igoring",replica_key);
        } else {
            let log = &self.config.storage().new_config();
             match FollowerReplicaState::new(self.config.id(),replica_msg.leader,&replica_key,&log).await {
                 Ok(replica_state) => {
                    self.followers_state.insert_replica(replica_state);
                 },
                 Err(err) => error!("error creating follower replica: {}, errr: {:#?}",replica_key,err)
             }
        }
    }

    /// send offset to leader, so it can chronize
    async fn sync_all_offsets_to_leader(&self, sink: &mut KfSink) {

        self.sync_offsets_to_leader(sink,self.followers_state.replica_offsets(&self.leader_id)).await;
    }

    /// send follower offset to leader
    async fn sync_offsets_to_leader(&self, sink: &mut KfSink,offsets: UpdateOffsetRequest) {

        let req_msg = RequestMessage::new_request(offsets)
            .set_client_id(format!("follower_id: {}",self.config.id()));

        trace!("sending offsets: {:#?} to leader: {}",&req_msg,self.leader_id);

        log_on_err!(sink.send_request(&req_msg).await,"error sending request to leader {}");
        debug!("synced follower offset: {} to leader: {}",self.config.id(),self.leader_id);
    }


}

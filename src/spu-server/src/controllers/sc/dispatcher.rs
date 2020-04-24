use std::time::Duration;
use std::process;
use std::io::Error as IoError;
use std::sync::Arc;

use log::info;
use log::trace;
use log::error;
use log::debug;
use log::warn;
use types::print_cli_err;

use futures::channel::mpsc::Receiver;
use futures::channel::mpsc::Sender;
use futures::channel::mpsc::channel;
use futures::StreamExt;
use futures::FutureExt;
use futures::select;
use futures::sink::SinkExt;

use flv_future_aio::task::spawn;
use flv_future_aio::timer::sleep;
use internal_api::InternalSpuApi;
use internal_api::InternalSpuRequest;
use internal_api::RegisterSpuRequest;
use internal_api::UpdateSpuRequest;
use internal_api::UpdateReplicaRequest;
use internal_api::UpdateAllRequest;
use internal_api::messages::Replica;
use kf_protocol::api::RequestMessage;
use kf_socket::KfSocket;
use kf_socket::KfSocketError;
use kf_socket::ExclusiveKfSink;
use flv_storage::FileReplica;
use flv_metadata::partition::ReplicaKey;
use types::log_on_err;
use flv_util::actions::Actions;

use crate::core::SharedGlobalContext;
use crate::core::SpecChange;
use crate::controllers::follower_replica::ReplicaFollowerController;
use crate::controllers::follower_replica::FollowerReplicaControllerCommand;
use crate::controllers::leader_replica::ReplicaLeaderController;
use crate::controllers::leader_replica::LeaderReplicaState;
use crate::controllers::leader_replica::LeaderReplicaControllerCommand;
use crate::InternalServerError;

use super::SupervisorCommand;

/// Controller for handling connection to SC
/// including registering and reconnect
pub struct ScDispatcher<S> {
    termination_receiver: Receiver<bool>,
    #[allow(dead_code)]
    termination_sender: Sender<bool>,
    #[allow(dead_code)]
    supervisor_command_sender: Sender<SupervisorCommand>,
    supervisor_command_receiver: Receiver<SupervisorCommand>,
    ctx: SharedGlobalContext<S>,
}

impl <S>ScDispatcher<S> {

    pub fn new(
        ctx: SharedGlobalContext<S>
    ) -> Self  {

        let (termination_sender,termination_receiver) = channel(1);
        let (supervisor_command_sender,supervisor_command_receiver) = channel(100);
        Self {
            termination_receiver,
            termination_sender,
            supervisor_command_sender,
            supervisor_command_receiver,
            ctx
        }
    }
}

impl ScDispatcher<FileReplica> {


    /// start the controller with ctx and receiver
    pub fn run(self) {

        spawn(self.dispatch_loop());
    }

    async fn dispatch_loop(mut self) {
        info!("starting SC Dispatcher");

        loop {
            if let Some(mut socket) = self.create_socket_to_sc().await {
                trace!(
                    "established connection to sc for spu: {}",
                    self.ctx.local_spu_id()
                );

                // register and exit on error
                match self.send_spu_registeration(&mut socket).await {
                    Ok(_) => {}
                    Err(err) => {
                        print_cli_err!(format!("cannot register with sc: {}", err));
                        process::exit(0x0100);
                    }
                }

                // continuously process updates from and send back status to SC
                match self.sc_request_loop(socket).await {
                    Ok(_) => {}
                    Err(err) => warn!("error, connecting to sc: {:#?}", err),
                }

                // We lost connection to sc.  Retry again
                // Currently we use 3 seconds to retry but this should be using backoff algorithm
                sleep(Duration::from_millis(3000)).await
                    
            }
        }
    }

    /// dispatch sc request
    async fn sc_request_loop(&mut self, socket: KfSocket) -> Result<(), KfSocketError> {
        let (sink, mut stream) = socket.split();
        let mut api_stream = stream.api_stream::<InternalSpuRequest, InternalSpuApi>();

        let shared_sink = Arc::new(ExclusiveKfSink::new(sink));
        loop {
            select! {
                sc_request = api_stream.next().fuse() => {

                    if let Some(sc_msg) = sc_request {
                       if let Ok(req_message) = sc_msg {
                            match req_message {
                                
                                InternalSpuRequest::UpdateAllRequest(request) => {
                                    if let Err(err) = self.handle_sync_all_request(request,shared_sink.clone()).await {
                                        error!("error handling all request from sc {}", err);
                                    }
                                },
                                InternalSpuRequest::UpdateReplicaRequest(request) => {
                                    if let Err(err) = self.handle_update_replica_request(request,shared_sink.clone()).await {
                                        error!("error handling update replica request: {}",err);
                                    }
                                },
                                InternalSpuRequest::UpdateSpuRequest(request) => {
                                    if let Err(err) = self.handle_update_spu_request(request,shared_sink.clone()).await {
                                        error!("error handling update spu request: {}",err);
                                    }
                                }
                            }
                            
                        } else {
                            debug!("no more sc msg content, end");
                            break;
                        }
                    
                    } else {
                        debug!("sc connection terminated");
                        break;
                    }
                    
                },
                super_command = self.supervisor_command_receiver.next().fuse() => {

                }

            }
            
        }

        Ok(())
    }

    /// register local spu to sc
    async fn send_spu_registeration(
        &self,
        socket: &mut KfSocket,
    ) -> Result<bool, InternalServerError> {

        let local_spu_id = self.ctx.local_spu_id();
       
        debug!("spu '{}' registration request", local_spu_id);

        let register_req = RegisterSpuRequest::new(local_spu_id);
        let mut message = RequestMessage::new_request(register_req);
        message
            .get_mut_header()
            .set_client_id(format!("spu: {}", local_spu_id));

        let response = socket.send(&message).await?;

        trace!("register response: {:#?}", response);

        let register_resp = &response.response;
        if register_resp.is_error() {
            warn!(
                "spu '{}' registration failed: {}",
                local_spu_id,
                register_resp.error_message()
            );

            Ok(false)
        } else {
            debug!("spu '{}' registration Ok", local_spu_id);

            Ok(true)
        }
    }

    /// connect to sc if can't connect try until we succeed
    /// or if we received termination message
    async fn create_socket_to_sc(&mut self) -> Option<KfSocket> {
        let spu_id = self.ctx.local_spu_id();
        let sc_endpoint = self.ctx.config().sc_endpoint().to_string();

         debug!("trying to connect to sc endpoint: {}",sc_endpoint);
       
        let wait_interval = self.ctx.config().sc_retry_ms;
        loop {
            trace!(
                "trying to create socket to sc: {:#?} for spu: {}",
                sc_endpoint,
                spu_id
            );
            let connect_future = KfSocket::connect(&sc_endpoint);

            select! {
                socket_res = connect_future.fuse() => {
                    match socket_res {
                        Ok(socket) => {
                            debug!("connected to sc for spu: {}",spu_id);
                            return Some(socket)
                        }
                        Err(err) => warn!("error connecting to sc: {}",err)
                    }

                    trace!("sleeping {} ms to connect to sc: {}",wait_interval,spu_id);
                    sleep(Duration::from_millis(wait_interval as u64)).await;
                },
                _ = self.termination_receiver.next() => {
                    info!("termination message received");
                    return None
                }
            }
        }
    }


    /// Bulk Update Handler sent by Controller
    ///
    async fn handle_sync_all_request(
        &mut self,
        req_msg: RequestMessage<UpdateAllRequest>,
        shared_sc_sink: Arc<ExclusiveKfSink>
    ) -> Result<(), IoError> {

        let (_, request) = req_msg.get_header_request();

        debug!(
            "RCVD-Req << (CTRL): UpdateAll({} spus, {} replicas)",
            request.spus.len(),
            request.replicas.len(),
        );


        let spu_actions = self.ctx.spu_localstore().sync_all(request.spus);

        trace!("all spu actions detail: {:#?}",spu_actions);

        /*
         * For now, there are nothing to do.
        for spu_action in spu_actions.into_iter() {
            
            match spu_action {

                SpecChange::Add(_) => {},
                SpecChange::Mod(_,_) => {},
                SpecChange::Delete(_) => {}

            }
            
        }
        */

        let replica_actions = self.ctx.replica_localstore().sync_all(request.replicas);
        self.apply_replica_actions(replica_actions,shared_sc_sink).await;    
        Ok(())
    }


    ///
    /// Follower Update Handler sent by a peer Spu
    ///
    async fn handle_update_replica_request(
        &mut self,
        req_msg: RequestMessage<UpdateReplicaRequest>,
        shared_sc_sink: Arc<ExclusiveKfSink>
    ) -> Result<(), IoError> {

        let (_, request) = req_msg.get_header_request();

        debug!("received replica update from sc: {:#?}",request);
    
        let replica_actions = self.ctx.replica_localstore().apply_changes(request.replicas().messages);
        self.apply_replica_actions(replica_actions,shared_sc_sink).await;    
        Ok(())
    }


    ///
    /// Follower Update Handler sent by a peer Spu
    ///
    async fn handle_update_spu_request(
        &mut self,
        req_msg: RequestMessage<UpdateSpuRequest>,
        _shared_sc_sink: Arc<ExclusiveKfSink>
    ) -> Result<(), IoError> {

        let (_, request) = req_msg.get_header_request();

        debug!("received spu update from sc: {:#?}",request);
    
        let _spu_actions = self.ctx.spu_localstore().apply_changes(request.spus());
        Ok(())
    }


    async fn apply_replica_actions(
        &self, 
        actions: Actions<SpecChange<Replica>>,
        shared_sc_sink: Arc<ExclusiveKfSink>
    ) {

        if actions.count() == 0 {
            debug!("no replica actions to process. ignoring");
            return;
        }

        trace!("applying replica leader {} actions", actions.count());

        let local_id = self.ctx.local_spu_id();

        for replica_action in actions.into_iter() {

            trace!("applying replica action: {:#?}", replica_action);

            match replica_action {
                SpecChange::Add(new_replica) => {
                    if new_replica.leader == local_id  {
                        self.add_leader_replica(
                            new_replica,
                            shared_sc_sink.clone()
                        )
                        .await;
                    } else {
                        self.add_follower_replica(
                            new_replica
                        ).await;
                    }
                },
                SpecChange::Delete(deleted_replica) => {
                    if deleted_replica.leader == local_id {
                        self.remove_leader_replica(&deleted_replica.id);
                    } else {
                        self.remove_follower_replica(deleted_replica);
                    }
                },
                SpecChange::Mod(new_replica,old_replica) => {
                    trace!("replica changed, old: {:#?}, new: {:#?}",new_replica,old_replica);

                    // check for leader change
                    if new_replica.leader != old_replica.leader {
                        if new_replica.leader == local_id  {
                            // we become leader
                            self.promote_replica(new_replica,old_replica,shared_sc_sink.clone());
                        } else {
                            // we are follower
                            // if we were leader before, we demote out self
                            if old_replica.leader == local_id {
                                 self.demote_replica(new_replica).await;
                            } else {
                                // we stay as follower but we switch to new leader
                                debug!("still follower but switching leader: {}",new_replica);
                                self.remove_follower_replica(old_replica);
                                self.add_follower_replica(new_replica).await;
                            }
                        }
                    } else {
                        if new_replica.leader == local_id  {
                            self.update_leader_replica(
                                new_replica
                            )
                            .await;
                        } else {
                            self.update_follower_replica(new_replica).await;
                        }
                    }
                }
            }

        }
    }


    async fn add_leader_replica(
        &self,
        replica: Replica,
        shared_sc_sink: Arc<ExclusiveKfSink>) 
    {

        debug!("adding new leader replica: {}",replica);

        let storage_log = self.ctx.config().storage().new_config();
        let replica_id = replica.id.clone();
                    
        match LeaderReplicaState::create_file_replica(replica, &storage_log).await {
            Ok(leader_replica) => {
                debug!("file replica for leader is created: {}",storage_log);
                self.spawn_leader_controller(replica_id,leader_replica,shared_sc_sink);
            },
            Err(err) => {
                error!("error creating storage foer leader replica {:#?}",err);
                // TODO: send status back to SC
            }
        }

    }


    async fn update_leader_replica(
        &self,
        replica: Replica,
    ) {
        debug!("updating leader controller: {}", replica.id);

        if self.ctx.leaders_state().has_replica(&replica.id) {
            
            debug!("leader replica was found, sending replica info: {}",replica);

            match self.ctx.leaders_state().send_message(
                &replica.id,
                LeaderReplicaControllerCommand::UpdateReplicaFromSc(replica.clone()),
            )
            .await
            {
                Ok(status) => {
                    if !status {
                        error!("leader controller mailbox: {} was not founded",replica.id);
                    }
                }
                Err(err) => error!("error sending external command: {:#?} to replica controller: {}", err,replica.id),
            }
        } else {
            error!("leader controller was not found: {}",replica.id)
        }
    }


    /// spwan new leader controller
    fn spawn_leader_controller(
        &self,
        replica_id: ReplicaKey,
        leader_state: LeaderReplicaState<FileReplica>,
        shared_sc_sink: Arc<ExclusiveKfSink>) 
    {

        debug!("spawning new leader controller for {}",replica_id);

        let (sender, receiver) = channel(10);
        
        if let Some(old_replica) = self.ctx.leaders_state().insert_replica(replica_id.clone(),leader_state, sender) {
            error!("there was existing replica when creating new leader replica: {}",old_replica.replica_id());
        }

        let leader_controller = ReplicaLeaderController::new(
            self.ctx.local_spu_id(),
            replica_id,
            receiver,
            self.ctx.leader_state_owned(),
            self.ctx.followers_sink_owned(),
            shared_sc_sink,
            self.ctx.offset_channel().sender()
        );
        leader_controller.run();

    }


    pub fn remove_leader_replica(
        &self,
        id: &ReplicaKey) {

        debug!("removing leader replica: {}", id);

        if self.ctx.leaders_state().remove_replica(id).is_none() {
            error!("fails to find leader replica: {} when removing",id);
        }
    }



    /// Promote follower replica as leader, 
    /// This is done in 3 steps
    /// // 1: Remove follower replica from followers state
    /// // 2: Terminate followers controller if need to be (if there are no more follower replicas for that controller)
    /// // 3: Start leader controller
    pub fn promote_replica(
        &self,
        new_replica: Replica,
        old_replica: Replica,
        shared_sc_sink: Arc<ExclusiveKfSink>) 
    {

         debug!("promoting replica: {} from: {}",new_replica,old_replica);

         if let Some(follower_replica) = self.ctx.followers_state().remove_replica(&old_replica.leader,&old_replica.id) {
                    
            debug!("old follower replica exists, converting to leader: {}",old_replica.id);

            let leader_state = LeaderReplicaState::new(
                    new_replica.id.clone(),
                    new_replica.leader,
                    follower_replica.storage_owned(),
                    new_replica.replicas
            );

            self.spawn_leader_controller(new_replica.id,leader_state,shared_sc_sink); 

        }
    }


    /// Demote leader replica as follower.
    /// This only happens on manual election
    pub async fn demote_replica(&self,replica: Replica) {

        debug!("demoting replica: {}",replica);

        if let Some(leader_replica_state) = self.ctx.leaders_state().remove_replica(&replica.id) {
            drop(leader_replica_state);
            // for now, we re-scan file replica
            self.add_follower_replica(replica).await;
        } else {
            error!("leader controller was not found: {}",replica.id)
        }
    }




    /// add new follower controller and it's mailbox
    async fn add_follower_replica(&self,replica: Replica) {

        let leader = &replica.leader;
        debug!("trying to adding follower replica: {}",replica);

        if let Some(mut sender) = self.ctx.followers_state().mailbox(leader) {
            debug!("existing follower controller exists: {}, send request to controller",replica);
             log_on_err!(sender.send(FollowerReplicaControllerCommand::AddReplica(replica)).await)
        } else {
            // we need to spin new follower controller
             debug!("no existing follower controller exists for {},need to spin up",replica);
            let (mut sender,receiver) = self.ctx.followers_state().insert_mailbox(*leader);
            let follower_controller = ReplicaFollowerController::new(
                *leader,
                receiver,
                self.ctx.spu_localstore_owned(),
                self.ctx.followers_state_owned(),
                self.ctx.config_owned()
            );
            follower_controller.run();
            log_on_err!(sender.send(FollowerReplicaControllerCommand::AddReplica(replica)).await);
        }
    }

    /// update follower replida
    async fn update_follower_replica(&self,replica: Replica) {

        let leader = &replica.leader;
        debug!("trying to adding follower replica: {}",replica);

        if let Some(mut sender) = self.ctx.followers_state().mailbox(leader) {
            debug!("existing follower controller exists: {}, send update request to controller",replica);
             log_on_err!(sender.send(FollowerReplicaControllerCommand::UpdateReplica(replica)).await)
        } else {
            error!("no follower controller found: {}",replica);
        }
    }


   

    fn remove_follower_replica(&self,replica: Replica) {

        debug!("removing follower replica: {}",replica);
        if self.ctx.followers_state().remove_replica(&replica.leader,&replica.id).is_none() {
            error!("there was no follower replica: {}",replica);
        }

    }


}

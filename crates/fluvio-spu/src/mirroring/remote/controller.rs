use std::{
    fmt,
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::select;
use tracing::{debug, error, info, instrument, warn};
use anyhow::{anyhow, Result};
use adaptive_backoff::prelude::{
    ExponentialBackoffBuilder, BackoffBuilder, ExponentialBackoff, Backoff,
};

use fluvio::config::TlsPolicy;
use futures_util::StreamExt;
use fluvio_controlplane_metadata::{
    mirror::{Home, MirrorPairStatus, MirrorType},
    partition::RemotePartitionConfig,
};
use fluvio_storage::{ReplicaStorage, FileReplica};
use fluvio_socket::{ClientConfig, FluvioSink, FluvioSocket};
use fluvio_spu_schema::{Isolation, server::mirror::StartMirrorRequest};
use fluvio_future::{net::DomainConnector, task::spawn, timer::sleep};
use fluvio_protocol::{record::Offset, api::RequestMessage};
use fluvio_types::event::offsets::OffsetChangeListener;

use crate::{
    control_plane::SharedMirrorStatusUpdate,
    core::{mirror::SharedMirrorLocalStore, GlobalContext},
    mirroring::remote::update_offsets::UpdateRemoteOffsetRequest,
    replication::leader::{FollowerNotifier, ReplicaOffsetRequest, SharedLeaderState},
};
use crate::mirroring::home::{
    home_api::HomeMirrorRequest, api_key::MirrorHomeApiEnum,
    update_offsets::UpdateHomeOffsetRequest,
};

use super::sync::{DefaultRemotePartitionSyncRequest, RemoteFilePartitionSyncRequest};

pub(crate) type SharedMirrorControllerState = Arc<MirrorControllerState>;

const MIRROR_RECONCILIATION_INTERVAL_SEC: u64 = 60; // 1 min

/// Metrics for mirror controller
#[derive(Debug)]
pub(crate) struct MirrorControllerMetrics {
    loop_count: AtomicU64,
    connect_count: AtomicU64,
    connect_failure: AtomicU64,
    home_leo: AtomicI64,
}

#[allow(dead_code)]
impl MirrorControllerMetrics {
    fn update_home_leo(&self, leo: Offset) {
        self.home_leo.store(leo, Ordering::SeqCst);
    }

    fn get_home_leo(&self) -> Offset {
        self.home_leo.load(Ordering::SeqCst)
    }

    fn increase_loop_count(&self) {
        self.loop_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_loop_count(&self) -> u64 {
        self.loop_count.load(Ordering::Relaxed)
    }

    fn increase_conn_count(&self) {
        self.connect_count.fetch_add(1, Ordering::Relaxed);
    }

    fn get_conn_count(&self) -> u64 {
        self.connect_count.load(Ordering::Relaxed)
    }

    fn increase_conn_failure(&self) {
        self.connect_failure.fetch_add(1, Ordering::Relaxed);
    }

    fn get_conn_failure(&self) -> u64 {
        self.connect_failure.load(Ordering::Relaxed)
    }
}

/// State for mirror controller which can be shared across tasks
#[derive(Debug)]
pub(crate) struct MirrorControllerState {
    metrics: MirrorControllerMetrics,
}

impl MirrorControllerState {
    pub(crate) fn new() -> Self {
        Self {
            metrics: MirrorControllerMetrics {
                loop_count: AtomicU64::new(0),
                home_leo: AtomicI64::new(-1), // -1 indicate this is unknown
                connect_count: AtomicU64::new(0),
                connect_failure: AtomicU64::new(0),
            },
        }
    }

    #[allow(dead_code)]
    pub(crate) fn get_metrics(&self) -> &MirrorControllerMetrics {
        &self.metrics
    }
}

const CLUSTER_LOOKUP_SEC: u64 = 5;

/// This controller run on mirror remote.
/// It's main responsbility is to synchronize mirror home from remote.
/// Remote will always initiate connection to home.
///
/// The synchronization activites are trigger by 2 events.
///
/// 1. Leader offset change due to new records
/// 2. Offset update event from home
///
/// Based on those events, controller will try update home with missing records.
/// Home will send periodic update event even if it has fully caught up with events.
pub(crate) struct MirrorRemoteToHomeController<S> {
    leader: SharedLeaderState<S>,
    remote_config: RemotePartitionConfig,
    state: Arc<MirrorControllerState>,
    mirror_store: SharedMirrorLocalStore,
    status_update: SharedMirrorStatusUpdate,
    max_bytes: u32,
    isolation: Isolation,
    follower_notifier: Arc<FollowerNotifier>,
}

impl<S> fmt::Debug for MirrorRemoteToHomeController<S>
where
    S: ReplicaStorage,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MirrorRemote {}->{}",
            self.leader.id(),
            self.remote_config.home_cluster
        )
    }
}

impl<S> MirrorRemoteToHomeController<S>
where
    S: ReplicaStorage + Sync + Send + 'static,
{
    pub(crate) fn run(
        ctx: &GlobalContext<FileReplica>,
        leader: SharedLeaderState<S>,
        remote_config: RemotePartitionConfig,
        isolation: Isolation,
        max_bytes: u32,
    ) -> SharedMirrorControllerState {
        debug!(
            isolation = ?isolation,
            max_bytes,
            "starting mirror remote controller {:#?}",remote_config);
        let state = Arc::new(MirrorControllerState::new());

        let controller = Self {
            leader,
            isolation,
            remote_config,
            state: state.clone(),
            max_bytes,
            mirror_store: ctx.mirrors_localstore_owned(),
            status_update: ctx.mirror_status_update_owned(),
            follower_notifier: ctx.follower_notifier_owned(),
        };
        spawn(controller.dispatch_loop());
        state
    }

    #[instrument()]
    async fn dispatch_loop(self) {
        let mut offset_listener = self.leader.offset_listener(&self.isolation);
        let mut backoff = create_backoff();

        loop {
            // first find home cluster
            if let Some(home) = self.find_home_cluster() {
                self.state.metrics.increase_loop_count();
                debug!(name = home.id, "found home cluster");
                let home_socket = self.create_socket_to_home(&mut backoff, &home).await;
                debug!("created socket to home");

                if self.remote_config.target {
                    if let Err(err) = self
                        .sync_mirror_as_target(&home, home_socket, &mut backoff)
                        .await
                    {
                        self.update_status(MirrorPairStatus::DetailFailure(err.to_string()))
                            .await
                            .unwrap();
                        error!("error syncing mirror loop {}", err);
                    }
                } else if let Err(err) = self
                    .sync_mirror_loop(&home, &mut offset_listener, home_socket, &mut backoff)
                    .await
                {
                    self.update_status(MirrorPairStatus::DetailFailure(err.to_string()))
                        .await
                        .unwrap();
                    error!("error syncing mirror loop {}", err);
                }
            } else {
                warn!("home cluster not found");
                sleep(Duration::from_secs(CLUSTER_LOOKUP_SEC)).await;
            }
        }
    }

    #[instrument(skip(home, leader_offset_listner, home_socket, tls, backoff))]
    async fn sync_mirror_loop(
        &self,
        home: &Home,
        leader_offset_listner: &mut OffsetChangeListener,
        (home_socket, tls): (FluvioSocket, bool),
        backoff: &mut ExponentialBackoff,
    ) -> Result<()> {
        debug!(home_id = home.id, "start syncing mirror as source");

        let (mut home_sink, mut home_stream) = home_socket.split();

        if tls {
            debug!("tls enabled, disabling zero copy sink");
            home_sink.disable_zerocopy();
        }

        let mut home_api_stream = home_stream.api_stream::<HomeMirrorRequest, MirrorHomeApiEnum>();

        self.send_initial_request(home, &mut home_sink).await?;

        // this flag is set to true, home need to be refreshed leader's offsets and any recordset.
        let mut home_updated_needed = false;

        // home_updated_needed triggers warning, despite being used in loop
        #[allow(unused)]
        loop {
            let home_leo = self.state.metrics.get_home_leo();

            debug!(home_leo, home_updated_needed, "waiting for next event");

            // update home if flag is set and we know what home leo is
            if home_updated_needed && home_leo >= 0 {
                self.update_remote_as_source(&mut home_sink, home_leo)
                    .await?;
                self.update_status(MirrorPairStatus::Successful).await?;
                home_updated_needed = false;
            }

            select! {
                _ = leader_offset_listner.listen() => {
                    info!("leader offset has changed, home cluster needs to be updated");
                    home_updated_needed = true;
                }

                msg = home_api_stream.next() => {
                    debug!("received response from home");
                    if let Some(req_msg_home) = msg {
                        let home_msg = req_msg_home?;

                        match home_msg {
                            HomeMirrorRequest::UpdateHomeOffset(req)=> {
                                home_updated_needed = self.update_from_home_as_source(req)?;
                            },
                            HomeMirrorRequest::SyncRecords(sync_request)=> {
                                return Err(anyhow!("received sync record request from home, this should not happen, since we are source"));
                            }
                         }
                        self.update_status(MirrorPairStatus::Successful).await?;
                        backoff.reset();
                    } else {
                        warn!("spu socket to home has terminated");
                        self.update_status(MirrorPairStatus::DetailFailure("closed connection".to_owned()))
                            .await?;
                        self.backoff_and_wait(backoff).await;
                        break;
                    }

                }
            }

            self.state.metrics.increase_conn_count();
        }

        info!("home has closed connection, terminating loop");

        Ok(())
    }

    #[instrument(skip(home, home_socket, tls, backoff))]
    // sync loop when this is source
    async fn sync_mirror_as_target(
        &self,
        home: &Home,
        (home_socket, tls): (FluvioSocket, bool),
        backoff: &mut ExponentialBackoff,
    ) -> Result<()> {
        info!(home_id = home.id, "start syncing mirror as target");

        let (mut home_sink, mut home_stream) = home_socket.split();

        if tls {
            debug!("tls enabled, disabling zero copy sink");
            home_sink.disable_zerocopy();
        }

        let mut home_api_stream = home_stream.api_stream::<HomeMirrorRequest, MirrorHomeApiEnum>();

        self.send_initial_request(home, &mut home_sink).await?;

        let mut paired: bool = false; // pairing status

        self.send_offsets_to_home_as_target(&mut home_sink).await?;

        // timer to update offsets to home
        let mut reconc_timer = sleep(Duration::from_secs(MIRROR_RECONCILIATION_INTERVAL_SEC));

        // home_updated_needed triggers warning, despite being used in loop
        #[allow(unused)]
        loop {
            select! {
                _ = &mut reconc_timer => {
                    info!("timer expired, sending reconciliation");
                    self.send_offsets_to_home_as_target(&mut home_sink).await?;
                    reconc_timer = sleep(Duration::from_secs(MIRROR_RECONCILIATION_INTERVAL_SEC));
                },

                msg = home_api_stream.next() => {
                    debug!("received response from home");
                    if let Some(req_msg_home) = msg {
                        let home_msg = req_msg_home?;

                        match home_msg {
                            HomeMirrorRequest::UpdateHomeOffset(req)=> {
                                return Err(anyhow!("received home offset request from home, this should not happen, since we are target"));
                            },
                            HomeMirrorRequest::SyncRecords(sync_request)=> {
                                if(!paired) {
                                    info!("sync received for the first time, indicating paired");
                                    self.update_status(MirrorPairStatus::Successful).await?;
                                    paired = true;
                                }

                                self.sync_record_from_home(sync_request.request.inner()).await?;
                                self.send_offsets_to_home_as_target(&mut home_sink).await?;
                            }
                         }
                        backoff.reset();
                    } else {
                        warn!("spu socket to home has terminated");
                        self.update_status(MirrorPairStatus::DetailFailure("closed connection".to_owned()))
                            .await?;
                        self.backoff_and_wait(backoff).await;
                        break;
                    }

                }
            }

            self.state.metrics.increase_conn_count();
        }

        info!("home has closed connection, terminating loop");

        Ok(())
    }

    async fn update_status(&self, pair_status: MirrorPairStatus) -> Result<()> {
        self.status_update
            .send_status(self.remote_config.home_cluster.clone(), pair_status)
            .await
    }

    async fn send_initial_request(&self, home: &Home, home_sink: &mut FluvioSink) -> Result<()> {
        // always starts with mirrong request
        // this is equivalent to register request
        // home should perform additional validation to ensure invalid edge request are rejected
        let start_mirror_request = RequestMessage::new_request(StartMirrorRequest {
            remote_cluster_id: home.remote_id.clone(),
            remote_replica: self.leader.id().to_string(),
        });

        info!(remote_id = home.remote_id, cluster = %self.leader.id(),"sending start mirror request");

        // send start mirror request
        home_sink
            .send_request(&start_mirror_request)
            .await
            .map_err(|err| err.into())
    }

    /// received new offset from home
    #[instrument(skip(req))]
    fn update_from_home_as_source(
        &self,
        req: RequestMessage<UpdateHomeOffsetRequest>,
    ) -> Result<bool> {
        let leader_leo = self.leader.leo();
        let old_home_leo = self.state.metrics.get_home_leo();
        let new_home_leo = req.request.leo;
        debug!(
            leader_leo,
            old_home_leo, new_home_leo, "received update from home"
        );
        // if old home leo is not initialized, we need to update home
        if old_home_leo < 0 {
            debug!(new_home_leo, "updating home leo from uninitialized");
            self.state.metrics.update_home_leo(new_home_leo);
        }
        match new_home_leo.cmp(&leader_leo) {
            std::cmp::Ordering::Greater => {
                // home leo should never be greater than leader's leo if this is not mirror target
                warn!(
                    leader_leo,
                    new_home_leo, "home has more records, this should not happen, this is error"
                );
                return Err(anyhow!("home's leo: {new_home_leo} > leader's leo: {leader_leo} this should not happen since this is target, this is error"));
            }
            std::cmp::Ordering::Less => {
                debug!(
                    new_home_leo,
                    leader_leo, "home has less records, need to refresh home"
                );
                self.state.metrics.update_home_leo(new_home_leo);
                Ok(true)
            }
            std::cmp::Ordering::Equal => {
                debug!(
                    new_home_leo,
                    "home has same records, no need to refresh home"
                );
                Ok(false)
            }
        }
    }

    /// look up home cluster from local store
    /// this may return None if remote cluster is send by SC by time controller is started
    fn find_home_cluster(&self) -> Option<Home> {
        let read = self.mirror_store.read();
        let mirror = read.get(&self.remote_config.home_cluster).cloned();

        drop(read);

        match mirror {
            Some(mirror) => match mirror.spec.mirror_type {
                MirrorType::Home(home) => Some(home),
                _ => None,
            },
            None => None,
        }
    }

    #[instrument]
    async fn update_remote_as_source(&self, sink: &mut FluvioSink, home_leo: Offset) -> Result<()> {
        debug!("updating home cluster");
        if let Some(sync_request) = self.geneate_remote_record_as_source(home_leo).await? {
            debug!(?sync_request, "home sync");
            let request = RequestMessage::new_request(sync_request)
                .set_client_id(format!("leader: {}", self.leader.id()));
            sink.encode_file_slices(&request, request.header.api_version())
                .await?;
            Ok(())
        } else {
            Ok(())
        }
    }

    /// remote is source, generate missing records to send to home
    async fn geneate_remote_record_as_source(
        &self,
        home_leo: Offset,
    ) -> Result<Option<RemoteFilePartitionSyncRequest>> {
        // leader off should be always greater than remote leo
        let leader_offset = self.leader.as_offset();

        // if remote mirror is all caught up, there is no need to send out update
        if leader_offset.leo == home_leo {
            debug!("home has caught up, just chilling out");
            return Ok(None);
        }

        let mut partition_response = RemoteFilePartitionSyncRequest {
            leo: leader_offset.leo,
            hw: leader_offset.hw,
            ..Default::default()
        };

        if leader_offset.leo > home_leo {
            match self
                .leader
                .read_records(home_leo, self.max_bytes, self.isolation)
                .await
            {
                // leader offset is greater than home, we need to send records to home (default)
                Ok(slice) => {
                    debug!(
                        hw = slice.end.hw,
                        leo = slice.end.leo,
                        replica = %self.leader.id(),
                        "read records"
                    );
                    if let Some(file_slice) = slice.file_slice {
                        partition_response.records = file_slice.into();
                    }
                    Ok(Some(partition_response))
                }
                Err(err) => {
                    error!(%err, "error reading records");
                    Err(anyhow!("error reading records: {}", err))
                }
            }
        } else {
            // home has more records, then we sync copy records from home
            debug!(
                hw = leader_offset.hw,
                leo = leader_offset.leo,
                home_leo,
                "oh no mirror home has more records"
            );
            Err(anyhow!(
                "leader has more records than home, this should not happen"
            ))
        }
    }

    #[instrument(skip(self, req))]
    async fn sync_record_from_home(
        &self,
        mut req: DefaultRemotePartitionSyncRequest,
    ) -> Result<()> {
        let append_flag = self
            .leader
            .append_record_set(&mut req.records, &self.follower_notifier)
            .await?;
        debug!(append_flag, "leader appended");
        Ok(())
    }

    /// create socket to home, this will always succeed
    #[instrument(skip(self, home))]
    async fn create_socket_to_home(
        &self,
        backoff: &mut ExponentialBackoff,
        home: &Home,
    ) -> (FluvioSocket, bool) {
        let tlspolicy = option_tlspolicy(home);

        loop {
            self.state.metrics.increase_conn_count();

            let endpoint = &self.remote_config.home_spu_endpoint;
            debug!(
                endpoint,
                attempt = self.state.metrics.get_conn_count(),
                "trying connect to home",
            );

            let home_config = if let Some(tlspolicy) = &tlspolicy {
                match DomainConnector::try_from(tlspolicy.clone()) {
                    Ok(connector) => ClientConfig::new(endpoint, connector, false),
                    Err(err) => {
                        error!(
                            "error establishing tls with leader at: <{}> err: {}",
                            endpoint, err
                        );
                        self.update_status(MirrorPairStatus::DetailFailure(err.to_string()))
                            .await
                            .unwrap();
                        self.backoff_and_wait(backoff).await;
                        continue;
                    }
                }
            } else {
                ClientConfig::with_addr(endpoint.to_string())
            };

            let home_config = home_config.with_prefix_sni_domain(&self.remote_config.home_spu_key);

            let res = home_config.connect().await;

            match res {
                Ok(versioned_socket) => {
                    let (socket, _config, _versions) = versioned_socket.split();
                    debug!("connected");
                    return (socket, tlspolicy.is_some());
                }

                Err(err) => {
                    error!("error connecting to leader at: <{}> err: {}", endpoint, err);
                    self.update_status(MirrorPairStatus::DetailFailure(err.to_string()))
                        .await
                        .unwrap();
                    self.backoff_and_wait(backoff).await;
                }
            }
        }
    }

    async fn backoff_and_wait(&self, backoff: &mut ExponentialBackoff) {
        let wait = backoff.wait();
        debug!(seconds = wait.as_secs(), "starting backing off, sleeping");
        sleep(wait).await;
        debug!("resume from backing off");
        self.state.metrics.increase_conn_failure();
    }

    // as target, send offset to home so it can sync records
    async fn send_offsets_to_home_as_target(&self, sink: &mut FluvioSink) -> Result<()> {
        let offset_request = ReplicaOffsetRequest {
            replica: self.leader.id().clone(),
            leo: self.leader.leo(),
            hw: self.leader.hw(),
        };

        debug!(?offset_request, "sending offset to home");
        let req_msg: RequestMessage<UpdateRemoteOffsetRequest> =
            RequestMessage::new_request(offset_request.into()).set_client_id("mirror home");

        sink.send_request(&req_msg).await?;

        Ok(())
    }
}

fn create_backoff() -> ExponentialBackoff {
    ExponentialBackoffBuilder::default()
        .factor(1.1)
        .min(Duration::from_secs(1))
        .max(Duration::from_secs(30))
        .build()
        .unwrap()
}

fn option_tlspolicy(home: &Home) -> Option<TlsPolicy> {
    use fluvio::config::{TlsCerts, TlsConfig};

    let ct = match &home.client_tls {
        Some(ct) => ct,
        _ => {
            return None;
        }
    };

    let certs = TlsCerts {
        domain: ct.domain.clone(),
        key: ct.client_key.clone(),
        cert: ct.client_cert.clone(),
        ca_cert: ct.ca_cert.clone(),
    };
    let tlscfg = TlsConfig::Inline(certs);
    Some(TlsPolicy::from(tlscfg))
}

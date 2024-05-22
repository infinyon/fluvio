use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};
use tracing::{debug, error, info, instrument, trace, warn};
use anyhow::{Result, anyhow};

use fluvio_auth::{AuthContext, InstanceAction};
use fluvio_controlplane_metadata::{
    extended::ObjectType,
    mirroring::{MirrorConnect, MirroringSpecWrapper, MirroringStatusResponse},
};
use fluvio_future::{task::spawn, timer::sleep};
use fluvio_protocol::api::{RequestHeader, ResponseMessage};
use fluvio_sc_schema::{
    core::MetadataItem,
    mirror::{ConnectionStatus, MirrorPairStatus, MirrorStatus, MirrorType},
    spu::SpuSpec,
    store::ChangeListener,
    topic::{MirrorConfig, ReplicaSpec, TopicSpec},
};
use fluvio_socket::ExclusiveFlvSink;
use fluvio_types::event::StickyEvent;

use crate::services::auth::AuthServiceContext;

// This is the entry point for handling mirroring requests
// Home clusters will receive requests from remote clusters
pub struct RemoteFetchingFromHomeController<AC: AuthContext, C: MetadataItem> {
    req: MirrorConnect,
    response_sink: ExclusiveFlvSink,
    end_event: Arc<StickyEvent>,
    header: RequestHeader,
    auth_ctx: Arc<AuthServiceContext<AC, C>>,
}

const MIRRORING_CONTROLLER_INTERVAL: u64 = 5;

impl<AC: AuthContext, C: MetadataItem> RemoteFetchingFromHomeController<AC, C> {
    pub fn start(
        req: MirrorConnect,
        response_sink: ExclusiveFlvSink,
        end_event: Arc<StickyEvent>,
        header: RequestHeader,
        auth_ctx: Arc<AuthServiceContext<AC, C>>,
    ) {
        let controller = Self {
            req: req.clone(),
            response_sink,
            end_event,
            header,
            auth_ctx,
        };

        spawn(controller.dispatch_loop());
    }

    #[instrument(skip(self), name = "RemoteFetchingFromHomeControllerLoop")]
    async fn dispatch_loop(mut self) {
        use tokio::select;

        // authorization check
        if let Ok(authorized) = self
            .auth_ctx
            .auth
            .allow_instance_action(
                ObjectType::Mirror,
                InstanceAction::Update,
                &self.req.remote_id,
            )
            .await
        {
            if !authorized {
                warn!("identity mismatch for remote_id: {}", self.req.remote_id);
                return;
            }
        }

        let ctx = self.auth_ctx.global_ctx.clone();

        // check if remote cluster exists
        let mirrors = ctx.mirrors().store().value(&self.req.remote_id).await;
        let remote = mirrors
            .iter()
            .find(|mirror| match &mirror.spec.mirror_type {
                MirrorType::Remote(r) => r.id == self.req.remote_id,
                _ => false,
            });

        if remote.is_none() {
            warn!("remote cluster not found: {}", self.req.remote_id);
            return;
        }
        info!(
            name = self.req.remote_id,
            "received mirroring connect request"
        );

        let mut topics_listener = ctx.topics().change_listener();
        let mut spus_listerner = ctx.spus().change_listener();

        loop {
            if self
                .sync_and_send_topics(&mut topics_listener, &mut spus_listerner)
                .await
                .is_err()
            {
                self.end_event.notify();
                break;
            }

            trace!("{}: waiting for changes", self.req.remote_id);
            select! {
                _ = self.end_event.listen() => {
                    debug!("connection has been terminated");
                    break;
                },

                _ = topics_listener.listen() => {
                    debug!("mirroring: {}, topic changes has been detected", self.req.remote_id);
                }

                _ = spus_listerner.listen() => {
                    debug!("mirroring: {}, spu changes has been detected", self.req.remote_id);
                }
            }

            // sleep for a while
            debug!("sleeping for {} seconds", MIRRORING_CONTROLLER_INTERVAL);
            sleep(Duration::from_secs(MIRRORING_CONTROLLER_INTERVAL)).await;
        }
    }

    #[instrument(skip(self, topics_listener))]
    async fn sync_and_send_topics(
        &mut self,
        topics_listener: &mut ChangeListener<TopicSpec, C>,
        spus_listener: &mut ChangeListener<SpuSpec, C>,
    ) -> Result<()> {
        if !topics_listener.has_change() && !spus_listener.has_change() {
            debug!("no changes, skipping");
            return Ok(());
        }
        let ctx = self.auth_ctx.global_ctx.clone();

        let spus = ctx.spus().store().clone_values().await;
        let topics = ctx.topics().store().clone_values().await;

        let mirror_topics = topics
            .into_iter()
            .filter_map(|topic| match topic.spec.replicas() {
                ReplicaSpec::Mirror(MirrorConfig::Home(h)) => {
                    let partition_id = h
                        .partitions()
                        .iter()
                        .position(|p| p.remote_cluster == self.req.remote_id);

                    match partition_id {
                        Some(id) => {
                            let replica_map = topic.status.replica_map;
                            let partition_id = id as u32;
                            let spu_id = replica_map
                                .get(&partition_id)
                                .and_then(|ids| ids.first().cloned());

                            match spu_id {
                                Some(spu_id) => {
                                    let spu_endpoint = spus
                                        .iter()
                                        .find(|s| s.spec.id == spu_id)
                                        .map(|s| s.spec.public_endpoint.addr())
                                        .unwrap_or_default();
                                    Some(MirroringSpecWrapper::new(
                                        topic.key.clone(),
                                        topic.spec,
                                        spu_id,
                                        spu_endpoint,
                                    ))
                                }
                                None => None,
                            }
                        }
                        None => None,
                    }
                }
                _ => None,
            })
            .collect::<Vec<_>>();

        match ctx.mirrors().store().value(&self.req.remote_id).await {
            Some(remote) => {
                let now = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis();

                let remote_name = remote.spec().to_string();
                let response = MirroringStatusResponse::new_ok(&remote_name, mirror_topics);
                let resp_msg = ResponseMessage::from_header(&self.header, response);

                // try to send response
                if let Err(err) = self
                    .response_sink
                    .send_response(&resp_msg, self.header.api_version())
                    .await
                {
                    let status = MirrorStatus::new(
                        MirrorPairStatus::Failed,
                        ConnectionStatus::Online,
                        now as u64,
                    );

                    ctx.mirrors()
                        .update_status(remote.key.clone(), status)
                        .await?;
                    error!(
                        "error mirroring sending {}, correlation_id: {}, err: {}",
                        self.req.remote_id,
                        self.header.correlation_id(),
                        err
                    );
                    return Err(anyhow!("error sending response, err: {}", err));
                }

                // update status
                let status = MirrorStatus::new(
                    MirrorPairStatus::Succesful,
                    ConnectionStatus::Online,
                    now as u64,
                );
                ctx.mirrors()
                    .update_status(remote.key.clone(), status)
                    .await?;

                return Ok(());
            }
            None => {
                error!("remote cluster not found: {}", self.req.remote_id);
                return Err(anyhow!("remote cluster not found: {}", self.req.remote_id));
            }
        }
    }
}

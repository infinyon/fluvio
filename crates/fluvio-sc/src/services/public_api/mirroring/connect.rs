use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};
use tracing::{debug, error, info, instrument, warn};
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
    mirror::{ConnectionStatus, MirrorPairStatus, MirrorSpec, MirrorStatus, MirrorType},
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

        if let Ok((_, status)) = self.get_remote_mirror().await {
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
                    if let Err(err) = self
                        .update_status(MirrorPairStatus::Unauthorized, status)
                        .await
                    {
                        error!("error updating status: {}", err);
                    }
                    warn!("identity mismatch for remote_id: {}", self.req.remote_id);
                    return;
                }
            }
        } else {
            // check if remote cluster exists
            error!("remote cluster not found: {}", self.req.remote_id);
            return;
        }

        let ctx = self.auth_ctx.global_ctx.clone();

        info!(
            name = self.req.remote_id,
            "received mirroring connect request"
        );

        let mut topics_listener = ctx.topics().change_listener();
        let mut spus_listerner = ctx.spus().change_listener();

        loop {
            if let Err(err) = self
                .sync_and_send_topics(&mut topics_listener, &mut spus_listerner)
                .await
            {
                error!("error syncing topics: {}", err);
                let remote = self.get_remote_mirror().await;
                if let Ok((_, status)) = remote {
                    if let Err(err) = self
                        .update_status(MirrorPairStatus::DetailFailure(err.to_string()), status)
                        .await
                    {
                        error!("error updating status: {}", err);
                    }
                }
                break;
            }

            select! {
                _ = self.end_event.listen() => {
                    info!("connection has been terminated");
                    let remote = self.get_remote_mirror().await;
                    if let Ok((_, status)) = remote {
                        if let Err(err) = self
                            .update_status(MirrorPairStatus::DetailFailure("connection closed".to_owned()), status)
                            .await
                        {
                            error!("error updating status: {}", err);
                        }
                    }
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

    async fn get_remote_mirror(&self) -> Result<(MirrorSpec, MirrorStatus)> {
        let ctx = self.auth_ctx.global_ctx.clone();
        let mirrors = ctx.mirrors().store().value(&self.req.remote_id).await;
        let remote = mirrors
            .iter()
            .find(|mirror| match &mirror.spec.mirror_type {
                MirrorType::Remote(r) => r.id == (self.req.remote_id),
                _ => false,
            });

        if let Some(remote) = remote {
            return Ok((
                remote.inner().spec().clone(),
                remote.inner().status().clone(),
            ));
        }

        warn!("remote cluster not found: {}", self.req.remote_id);
        Err(anyhow!("remote cluster not found: {}", self.req.remote_id))
    }

    #[instrument(skip(self, topics_listener, spus_listener))]
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
                                    if let Some(spu) = spus.iter().find(|s| s.spec.id == spu_id) {
                                        Some(MirroringSpecWrapper::new(
                                            topic.key.clone(),
                                            topic.spec,
                                            spu_id,
                                            spu.spec().public_endpoint.addr(),
                                            spu.key().to_string(),
                                        ))
                                    } else {
                                        None
                                    }
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
                let remote_name = remote.spec().to_string();
                let status = remote.inner().status().clone();
                let response = MirroringStatusResponse::new_ok(&remote_name, mirror_topics);
                let resp_msg = ResponseMessage::from_header(&self.header, response);

                // try to send response
                if let Err(err) = self
                    .response_sink
                    .send_response(&resp_msg, self.header.api_version())
                    .await
                {
                    return Err(anyhow!("error sending response, err: {}", err));
                }

                // update status
                self.update_status(MirrorPairStatus::Successful, status)
                    .await?;

                return Ok(());
            }
            None => {
                return Err(anyhow!("remote cluster not found: {}", self.req.remote_id));
            }
        }
    }

    async fn update_status(
        &self,
        pair_status: MirrorPairStatus,
        status: MirrorStatus,
    ) -> Result<()> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_millis();

        let new_status =
            MirrorStatus::new(pair_status.clone(), ConnectionStatus::Online, now as u64);
        let mut status = status.clone();
        status.merge_from_sc(new_status);

        self.auth_ctx
            .global_ctx
            .mirrors()
            .update_status(self.req.remote_id.clone(), status)
            .await?;

        Ok(())
    }
}

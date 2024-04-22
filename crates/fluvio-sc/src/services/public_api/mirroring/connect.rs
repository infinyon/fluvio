use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};
use fluvio_controlplane_metadata::mirroring::{
    MirrorConnect, MirroringStatusResponse, MirroringSpecWrapper,
};
use fluvio_future::{task::spawn, timer::sleep};
use fluvio_protocol::api::{RequestHeader, ResponseMessage};
use fluvio_sc_schema::{
    core::MetadataItem,
    mirror::{ConnectionStatus, MirrorPairStatus, MirrorStatus},
    store::ChangeListener,
    topic::{MirrorConfig, ReplicaSpec, TopicSpec},
};
use fluvio_socket::ExclusiveFlvSink;
use fluvio_types::event::StickyEvent;
use tracing::{debug, error, info, instrument, trace};
use anyhow::{Result, anyhow};

use crate::core::Context;

// This is the entry point for handling mirroring requests
// Home clusters will receive requests from remote clusters
pub struct MirroringConnectController<C: MetadataItem> {
    req: MirrorConnect,
    response_sink: ExclusiveFlvSink,
    end_event: Arc<StickyEvent>,
    ctx: Arc<Context<C>>,
    header: RequestHeader,
}

const MIRRORING_CONTROLLER_INTERVAL: u64 = 5;

impl<C: MetadataItem> MirroringConnectController<C> {
    pub fn start(
        req: MirrorConnect,
        response_sink: ExclusiveFlvSink,
        end_event: Arc<StickyEvent>,
        ctx: Arc<Context<C>>,
        header: RequestHeader,
    ) {
        let controller = Self {
            req: req.clone(),
            response_sink,
            end_event,
            ctx,
            header,
        };

        spawn(controller.dispatch_loop());
    }

    #[instrument(skip(self), name = "MirroringConnectControllerLoop")]
    async fn dispatch_loop(mut self) {
        use tokio::select;
        info!(name = self.req.name, "received mirroring connect request");

        let mut topics_listener = self.ctx.topics().change_listener();

        loop {
            if self
                .sync_and_send_topics(&mut topics_listener)
                .await
                .is_err()
            {
                self.end_event.notify();
                break;
            }

            trace!("{}: waiting for changes", self.req.name);
            select! {
                _ = self.end_event.listen() => {
                    debug!("connection has been terminated");
                    break;
                },

                _ = topics_listener.listen() => {
                    debug!("mirroring: {}, topic changes has been detected", self.req.name);
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
    ) -> Result<()> {
        if !topics_listener.has_change() {
            debug!("no changes, skipping");
            return Ok(());
        }

        let topics = self.ctx.topics().store().clone_values().await;
        let mirror_topics = topics
            .into_iter()
            .map(|topic| {
                MirroringSpecWrapper::new(topic.key.clone(), topic.spec, topic.status.replica_map)
            })
            .filter(|topic| match topic.spec.replicas() {
                ReplicaSpec::Mirror(MirrorConfig::Home(t)) => t
                    .partitions()
                    .iter()
                    .any(|p| p.remote_cluster == self.req.name),
                _ => false,
            })
            .collect::<Vec<_>>();

        match self.ctx.mirrors().store().value(&self.req.name).await {
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

                    self.ctx
                        .mirrors()
                        .update_status(remote.key.clone(), status)
                        .await?;
                    error!(
                        "error mirroring sending {}, correlation_id: {}, err: {}",
                        self.req.name,
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
                self.ctx
                    .mirrors()
                    .update_status(remote.key.clone(), status)
                    .await?;

                return Ok(());
            }
            None => {
                error!("remote cluster not found: {}", self.req.name);
                return Err(anyhow!("remote cluster not found: {}", self.req.name));
            }
        }
    }
}

use fluvio_controlplane_metadata::partition::Replica;
use std::sync::Arc;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::time::Duration;
use std::time::Instant;

use tracing::error;
use tracing::{debug,trace};
use tracing::instrument;
use async_trait::async_trait;
use async_channel::Sender;
use futures_util::stream::Stream;

use fluvio_types::SpuId;
use fluvio_future::net::TcpStream;
use dataplane::api::RequestMessage;
use fluvio_controlplane_metadata::spu::store::SpuLocalStorePolicy;
use fluvio_service::{FlvService, wait_for_request};
use fluvio_socket::{FlvSocket, FlvSocketError, FlvSink};
use fluvio_controlplane::{
    InternalScRequest, InternalScKey, RegisterSpuResponse, UpdateLrsRequest, UpdateReplicaRequest,
    UpdateSpuRequest,
};
use fluvio_controlplane_metadata::message::{ReplicaMsg, Message, SpuMsg};

use crate::core::SharedContext;
use crate::stores::{K8ChangeListener};
use crate::stores::partition::{PartitionSpec, PartitionStatus, PartitionResolution};
use crate::stores::spu::SpuSpec;
use crate::controllers::spus::SpuAction;
use crate::stores::actions::WSAction;

const HEALTH_DURATION: u64 = 30;

#[derive(Debug)]
pub struct ScInternalService {}

impl ScInternalService {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl FlvService<TcpStream> for ScInternalService {
    type Context = SharedContext;
    type Request = InternalScRequest;

    async fn respond(
        self: Arc<Self>,
        context: SharedContext,
        socket: FlvSocket,
    ) -> Result<(), FlvSocketError> {
        let (mut sink, mut stream) = socket.split();
        let mut api_stream = stream.api_stream::<InternalScRequest, InternalScKey>();

        // every SPU need to be validated and registered
        let spu_id = wait_for_request!(api_stream,
            InternalScRequest::RegisterSpuRequest(req_msg) => {
                let spu_id = req_msg.request.spu();
                let mut status = true;
                debug!("registration req from spu '{}'", spu_id);


                let register_res = if context.spus().store().validate_spu_for_registered(spu_id).await {
                    debug!("SPU: {} validation succeed",spu_id);
                    RegisterSpuResponse::ok()
                } else {
                    status = false;
                    debug!("SPU: {} validation failed",spu_id);
                    RegisterSpuResponse::failed_registeration()
                };

                let response = req_msg.new_response(register_res);
                sink.send_response(&response,req_msg.header.api_version()).await?;

                if !status {
                    return Ok(())
                }

                spu_id
            }
        );

        debug!("beginning SPU loop: {}", spu_id);
        let health_sender = context.health().sender();

        health_sender
            .send(SpuAction::up(spu_id))
            .await
            .map_err(|err| {
                IoError::new(
                    ErrorKind::BrokenPipe,
                    format!("unable to send health status: {}", err),
                )
            })?;

        if let Err(err) =
            dispatch_loop(context, spu_id, api_stream, sink, health_sender.clone()).await
        {
            error!("error with SPU <{}>, error: {}", spu_id, err);
        }

        debug!("connection to SPU is terminated, send off");
        health_sender
            .send(SpuAction::down(spu_id))
            .await
            .map_err(|err| {
                IoError::new(
                    ErrorKind::BrokenPipe,
                    format!("unable to send health status: {}", err),
                )
            })?;

        Ok(())
    }
}

// perform internal dispatch
async fn dispatch_loop(
    context: SharedContext,
    spu_id: SpuId,
    mut api_stream: impl Stream<Item = Result<InternalScRequest, FlvSocketError>> + Unpin,
    mut sink: FlvSink,
    health_sender: Sender<SpuAction>,
) -> Result<(), FlvSocketError> {
   

    let mut time_left = Duration::from_secs(HEALTH_DURATION);

    let mut spu_spec_listener = context.spus().change_listener();
    let mut partition_spec_listener = context.partitions().change_listener();

    loop {
        use tokio::select;
        use futures_util::stream::StreamExt;
        use fluvio_future::timer::sleep;

        let health_time = Instant::now();
        debug!(
            "waiting for SPU channel or updates {}, health check left: {} secs",
            spu_id,
            time_left.as_secs()
        );

        send_spu_spec_changes(&mut spu_spec_listener, &mut sink, spu_id).await?;
        send_replica_spec_changes(&mut partition_spec_listener, &mut sink, spu_id).await?;

        trace!("waiting for events");

        select! {

            _ = sleep(time_left) => {
                debug!("send spu health up");
                health_sender
                    .send(SpuAction::up(spu_id))
                    .await
                    .map_err(|err| {
                        IoError::new(
                            ErrorKind::BrokenPipe,
                            format!("unable to send health status: {}", err),
                        )
                    })?;
                time_left = Duration::from_secs(HEALTH_DURATION);
            },

            spu_request_msg = api_stream.next() =>  {


                if let Some(spu_request) = spu_request_msg {
                    tracing::trace!("received spu: {} request: {:#?}",spu_id,spu_request);

                    if let Ok(req_message) = spu_request {
                        match req_message {
                            InternalScRequest::UpdateLrsRequest(msg) => {
                                debug!("received lrs request: {}",msg);
                                receive_lrs_update(&context,msg.request).await;
                            },
                            InternalScRequest::RegisterSpuRequest(msg) => {
                                error!("registration req only valid during initialization: {:#?}",msg);
                                return Err(IoError::new(ErrorKind::InvalidData,"register spu request is only valid at init").into())
                            },
                            InternalScRequest::ReplicaRemovedRequest(msg) => {

                            }
                        }
                    } else {
                        debug!("no spu content: {}",spu_id);
                    }
                } else {
                    debug!("end of connection to spu: {}",spu_id);
                    break;
                }

                time_left -= health_time.elapsed();
            },


            _ = spu_spec_listener.listen() => {
                debug!("spu spec changed");
            },

            _ = partition_spec_listener.listen() => {
                debug!("partition spec changed");
            }

        }
    }

    Ok(())
}

/// send lrs update to metadata stores
async fn receive_lrs_update(ctx: &SharedContext, requests: UpdateLrsRequest) {
    let read_guard = ctx.partitions().store().read().await;
    for lrs_req in requests.into_requests().into_iter() {
        let action = if let Some(partition) = read_guard.get(&lrs_req.id) {
            let mut current_status = partition.inner().status().clone();
            let key = lrs_req.id.clone();
            let new_status = PartitionStatus::new2(
                lrs_req.leader,
                lrs_req.replicas,
                PartitionResolution::Online,
            );
            current_status.merge(new_status);

            WSAction::UpdateStatus::<PartitionSpec>((key, current_status))
        } else {
            error!(
                "trying to update replica: {}, that doesn't exist",
                lrs_req.id
            );
            return;
        };

        ctx.partitions().send_action(action).await;
    }

    drop(read_guard);
}

/// send spu spec changes only
#[instrument(skip(sink))]
async fn send_spu_spec_changes(
    listener: &mut K8ChangeListener<SpuSpec>,
    sink: &mut FlvSink,
    spu_id: SpuId,
) -> Result<(), FlvSocketError> {
    if !listener.has_change() {
        debug!("changes is empty, skipping");
        return Ok(());
    }

    let changes = listener.sync_spec_changes().await;
    if changes.is_empty() {
        debug!("spec changes is empty, skipping");
        return Ok(());
    }

    let epoch = changes.epoch;
    let is_sync_all = changes.is_sync_all();
    let (updates, deletes) = changes.parts();
    let request = if is_sync_all {
        UpdateSpuRequest::with_all(epoch, updates.into_iter().map(|u| u.spec).collect())
    } else {
        let mut changes: Vec<SpuMsg> = updates
            .into_iter()
            .map(|v| Message::update(v.spec))
            .collect();
        let mut deletes = deletes
            .into_iter()
            .map(|d| Message::delete(d.spec))
            .collect();
        changes.append(&mut deletes);
        UpdateSpuRequest::with_changes(epoch, changes)
    };

    let mut message = RequestMessage::new_request(request);
    message.get_mut_header().set_client_id("sc");

    debug!(
        "sending to spu: {}, all: {}, changes: {}",
        spu_id,
        message.request.all.len(),
        message.request.changes.len()
    );
    sink.send_request(&message).await?;
    Ok(())
}

#[instrument(skip(sink))]
async fn send_replica_spec_changes(
    listener: &mut K8ChangeListener<PartitionSpec>,
    sink: &mut FlvSink,
    spu_id: SpuId,
) -> Result<(), FlvSocketError> {

    use crate::stores::ChangeFlag;

    if !listener.has_change() {
        debug!("changes is empty, skipping");
        return Ok(());
    }

    let changes = listener.sync_changes_with_filter(&ChangeFlag{
        spec: true,
        status: false,
        meta: true
    }).await;
    if changes.is_empty() {
        debug!("spec changes is empty, skipping");
        return Ok(());
    }

    let epoch = changes.epoch;

    let is_sync_all = changes.is_sync_all();
    let (updates, deletes) = changes.parts();

    let request = if is_sync_all {
        UpdateReplicaRequest::with_all(
            epoch,
            updates
                .into_iter()
                .map(|partition| {
                    let replica: Replica = partition.into();
                    replica
                })
                .collect(),
        )
    } else {
        let mut changes: Vec<ReplicaMsg> = updates
            .into_iter()
            .map(|partition| {
                let replica: Replica = partition.into();
                Message::update(replica)
            })
            .collect();
        let mut deletes = deletes
            .into_iter()
            .map(|partition| {
                let replica: Replica = partition.into();
                Message::delete(replica)
            })
            .collect();
        changes.append(&mut deletes);
        UpdateReplicaRequest::with_changes(epoch, changes)
    };

    let mut message = RequestMessage::new_request(request);
    message.get_mut_header().set_client_id("sc");

    debug!(
        "sending to spu: {}, all: {}, changes: {}",
        spu_id,
        message.request.all.len(),
        message.request.changes.len()
    );
    sink.send_request(&message).await?;
    Ok(())
}


use fluvio_controlplane_metadata::partition::Replica;
use std::sync::Arc;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::time::Duration;
use std::time::Instant;

use tracing::error;
use tracing::{debug, info, instrument};
use async_trait::async_trait;
use async_channel::Sender;
use futures_util::stream::Stream;

use fluvio_types::SpuId;
use dataplane::api::RequestMessage;
use fluvio_controlplane_metadata::spu::store::SpuLocalStorePolicy;
use fluvio_service::{FlvService, wait_for_request};
use fluvio_socket::{FluvioSocket, SocketError, FluvioSink};
use fluvio_controlplane::{
    InternalScRequest, InternalScKey, RegisterSpuResponse, UpdateLrsRequest, UpdateReplicaRequest,
    UpdateSpuRequest, ReplicaRemovedRequest,
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
impl FlvService for ScInternalService {
    type Context = SharedContext;
    type Request = InternalScRequest;

    async fn respond(
        self: Arc<Self>,
        context: SharedContext,
        socket: FluvioSocket,
    ) -> Result<(), SocketError> {
        let (mut sink, mut stream) = socket.split();
        let mut api_stream = stream.api_stream::<InternalScRequest, InternalScKey>();

        // every SPU need to be validated and registered
        let spu_id = wait_for_request!(api_stream,
            InternalScRequest::RegisterSpuRequest(req_msg) => {
                let spu_id = req_msg.request.spu();
                let mut status = true;
                debug!(spu_id,"registration req");

                let register_res = if context.spus().store().validate_spu_for_registered(spu_id).await {
                    RegisterSpuResponse::ok()
                } else {
                    status = false;
                    debug!(spu_id,"spu validation failed");
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

        info!(spu_id, "SPU connected");

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
        debug!("send connection health up");

        if let Err(err) =
            dispatch_loop(context, spu_id, api_stream, sink, health_sender.clone()).await
        {
            error!("error with SPU <{}>, error: {}", spu_id, err);
        }

        info!(spu_id, "SPU terminated");

        health_sender
            .send(SpuAction::down(spu_id))
            .await
            .map_err(|err| {
                IoError::new(
                    ErrorKind::BrokenPipe,
                    format!("unable to send health status: {}", err),
                )
            })?;

        debug!("send connection health down");

        Ok(())
    }
}

// perform internal dispatch
#[instrument(name = "ScInternalService", skip(context, api_stream, health_sender))]
async fn dispatch_loop(
    context: SharedContext,
    spu_id: SpuId,
    mut api_stream: impl Stream<Item = Result<InternalScRequest, SocketError>> + Unpin,
    mut sink: FluvioSink,
    health_sender: Sender<SpuAction>,
) -> Result<(), SocketError> {
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

        debug!("waiting for events");

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
                                receive_replica_remove(&context,msg.request).await;
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
    let mut actions = vec![];
    let read_guard = ctx.partitions().store().read().await;
    for lrs_req in requests.into_requests().into_iter() {
        if let Some(partition) = read_guard.get(&lrs_req.id) {
            let mut current_status = partition.inner().status().clone();
            let key = lrs_req.id.clone();
            let new_status = PartitionStatus::new2(
                lrs_req.leader,
                lrs_req.replicas,
                PartitionResolution::Online,
            );
            current_status.merge(new_status);

            actions.push(WSAction::UpdateStatus::<PartitionSpec>((
                key,
                current_status,
            )));
        } else {
            error!(
                "trying to update replica: {}, that doesn't exist",
                lrs_req.id
            );
            return;
        }
    }

    drop(read_guard);

    for action in actions.into_iter() {
        ctx.partitions().send_action(action).await;
    }
}

#[instrument(
    skip(ctx,request),
    fields(replica=%request.id)
)]
async fn receive_replica_remove(ctx: &SharedContext, request: ReplicaRemovedRequest) {
    debug!(request=?request);
    // create action inside to optimize read locking
    let read_guard = ctx.partitions().store().read().await;
    let delete_action = if read_guard.contains_key(&request.id) {
        // force to delete partition regardless if confirm
        if request.confirm {
            debug!("force delete");
            Some(WSAction::DeleteFinal::<PartitionSpec>(request.id))
        } else {
            debug!("no delete");
            None
        }
    } else {
        error!("replica doesn't exist");
        None
    };

    drop(read_guard);

    if let Some(action) = delete_action {
        ctx.partitions().send_action(action).await;
    }
}

/// send spu spec changes only
#[instrument(skip(sink))]
async fn send_spu_spec_changes(
    listener: &mut K8ChangeListener<SpuSpec>,
    sink: &mut FluvioSink,
    spu_id: SpuId,
) -> Result<(), SocketError> {
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
        spu_id,
        all = message.request.all.len(),
        changes = message.request.changes.len(),
        "sending to spu",
    );
    sink.send_request(&message).await?;
    Ok(())
}

#[instrument(skip(sink))]
async fn send_replica_spec_changes(
    listener: &mut K8ChangeListener<PartitionSpec>,
    sink: &mut FluvioSink,
    spu_id: SpuId,
) -> Result<(), SocketError> {
    use crate::stores::ChangeFlag;

    if !listener.has_change() {
        debug!("changes is empty, skipping");
        return Ok(());
    }

    // we are only interested in spec changes or metadata changes
    // not rely on partition status deleted because partition status contains offset changes which we don't want
    let changes = listener
        .sync_changes_with_filter(&ChangeFlag {
            spec: true,
            status: false,
            meta: true,
        })
        .await;
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

    debug!(?request, "sending replica to spu");

    let mut message = RequestMessage::new_request(request);
    message.get_mut_header().set_client_id("sc");

    sink.send_request(&message).await?;
    Ok(())
}

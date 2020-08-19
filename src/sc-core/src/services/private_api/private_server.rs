use std::sync::Arc;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::time::Duration;
use std::time::Instant;

use tracing::error;
use tracing::debug;
use async_trait::async_trait;
use async_channel::Sender;
use futures::Stream;

use flv_types::SpuId;
use flv_future_aio::net::TcpStream;
use kf_protocol::api::*;
use flv_metadata_cluster::store::Epoch;
use flv_metadata_cluster::spu::store::SpuLocalStorePolicy;
use kf_service::KfService;
use kf_service::wait_for_request;
use kf_socket::*;
use internal_api::*;

use crate::core::*;
use crate::stores::partition::*;
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
impl KfService<TcpStream> for ScInternalService {
    type Context = SharedContext;
    type Request = InternalScRequest;

    async fn respond(
        self: Arc<Self>,
        context: SharedContext,
        socket: KfSocket,
    ) -> Result<(), KfSocketError> {
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
    mut api_stream: impl Stream<Item = Result<InternalScRequest, KfSocketError>> + Unpin,
    mut sink: KfSink,
    health_sender: Sender<SpuAction>,
) -> Result<(), KfSocketError> {
    let mut spu_epoch = context.spus().store().init_epoch().epoch();
    let mut partition_epoch = context.partitions().store().init_epoch().epoch();

    // send initial spu and replicas
    spu_epoch = send_spu_change(spu_epoch, &context, &mut sink, spu_id).await?;
    partition_epoch = send_replica_change(partition_epoch, &context, &mut sink, spu_id).await?;

    // we wait for update from SPU or wait for updates form SPU channel

    let mut time_left = Duration::from_secs(HEALTH_DURATION);

    loop {
        use tokio::select;
        use futures::stream::StreamExt;
        use flv_future_aio::timer::sleep;

        let health_time = Instant::now();
        debug!(
            "waiting for SPU channel or updates {}, health check left: {} secs",
            spu_id,
            time_left.as_secs()
        );

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
                                send_lrs_update(&context,msg.request).await;
                            },
                            InternalScRequest::RegisterSpuRequest(msg) => {
                                error!("registration req only valid during initialization: {:#?}",msg);
                                return Err(IoError::new(ErrorKind::InvalidData,"register spu request is only valid at init").into())
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


            _ = context.spus().listen() => {

                debug!("spu store changed: {}",spu_epoch);
                spu_epoch = send_spu_change(spu_epoch, &context, &mut sink,spu_id).await?;

            },

            _ = context.partitions().listen() => {
                debug!("partition store changed: {}",partition_epoch);
                partition_epoch = send_replica_change(partition_epoch, &context, &mut sink,spu_id).await?;
            }

        }
    }

    Ok(())
}

/// send lrs update to metadata stores
async fn send_lrs_update(ctx: &SharedContext, lrs_req: UpdateLrsRequest) {
    let read_guard = ctx.partitions().store().read().await;
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

    drop(read_guard);
    ctx.partitions().send_action(action).await;
}

async fn send_spu_change(
    epoch: Epoch,
    ctx: &SharedContext,
    sink: &mut KfSink,
    spu_id: SpuId,
) -> Result<Epoch, KfSocketError> {
    use flv_metadata_cluster::message::*;

    let read_guard = ctx.spus().store().read().await;
    let changes = read_guard.changes_since(epoch);
    drop(read_guard);

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
    Ok(epoch)
}

async fn send_replica_change(
    epoch: Epoch,
    ctx: &SharedContext,
    sink: &mut KfSink,
    spu_id: SpuId,
) -> Result<Epoch, KfSocketError> {
    use flv_metadata_cluster::message::*;

    let read_guard = ctx.partitions().store().read().await;
    let changes = read_guard.changes_since(epoch);
    drop(read_guard);

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
    Ok(epoch)
}

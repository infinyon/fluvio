use std::time::Duration;
use std::io::Error as IoError;

use tracing::{debug, info, error, instrument};

use fluvio_future::timer::sleep;
use fluvio_stream_model::core::MetadataItem;
use fluvio_future::task::spawn;

use crate::core::SharedContext;
use crate::stores::StoreContext;
use crate::stores::spu::*;

/// Reconcile SPU's health status with the health check cache
pub struct SpuHealthCheckController<C: MetadataItem> {
    spus: StoreContext<SpuSpec, C>,
    health_check: SharedHealthCheck,
    counter: u64, // how many time we have been sync
}

impl<C: MetadataItem + 'static> SpuHealthCheckController<C> {
    pub fn start(ctx: SharedContext<C>) {
        let controller = Self {
            spus: ctx.spus().clone(),
            health_check: ctx.health().clone(),
            counter: 0,
        };

        info!("starting spu controller");
        spawn(controller.dispatch_loop());
    }

    #[instrument(skip(self), name = "SpuControllerLoop")]
    async fn dispatch_loop(self) {
        info!("started");
        loop {
            if let Err(err) = self.inner_loop().await {
                error!("error with inner loop: {:#?}", err);
                debug!("sleeping 10 seconds try again");
                sleep(Duration::from_secs(10)).await;
            }
        }
    }

    #[instrument(skip(self))]
    async fn inner_loop(&self) -> Result<(), IoError> {
        use tokio::select;

        debug!("initializing listeners");

        let mut health_listener = self.health_check.listener();
        debug!("finished initializing listeners");

        loop {
            self.sync_store().await?;

            select! {
                _ = health_listener.listen() => {
                    debug!("detected changes in health listener");

                },
            }
        }
    }

    /// sync spu status with store
    #[instrument(skip(self),fields(counter=self.counter))]
    async fn sync_store(&self) -> Result<(), IoError> {
        // first get status values
        let spus = self.spus.store().clone_values().await;

        let mut changes = vec![];
        let health_read = self.health_check.read().await;

        for mut spu in spus.into_iter() {
            // check if we need to sync spu and our health check cache

            let spu_id = spu.spec.id;

            // check if we have status
            if let Some(health_status) = health_read.get(&spu_id) {
                // if status is init, we can set health
                if spu.status.is_init() {
                    debug!("SPU is in init state setting health status");
                    if *health_status {
                        debug!("health status is ok, setting spu online");
                        spu.status.set_online();
                    } else {
                        debug!("health status is bad, setting spu offline");
                        spu.status.set_offline();
                    }
                    info!(id = spu.spec.id, status = %spu.status,"init => health status change");
                    changes.push(spu);
                } else {
                    debug!("SPU's status was already set, checking if we need to update");
                    // change if health is different
                    let old_status = spu.status.is_online();
                    if old_status != *health_status {
                        if *health_status {
                            spu.status.set_online();
                        } else {
                            spu.status.set_offline();
                        }
                        info!(id = spu.spec.id, status = %spu.status,"update health status");
                        changes.push(spu);
                    } else {
                        debug!(id = spu.spec.id, status = %spu.status,"ignoring health status");
                    }
                }
            }
        }

        drop(health_read);

        for updated_spu in changes.into_iter() {
            let key = updated_spu.key;
            let status = updated_spu.status;
            debug!(id = updated_spu.spec.id, status = %status, "updating spu status");
            self.spus.update_status(key, status).await?;
        }

        Ok(())
    }
}

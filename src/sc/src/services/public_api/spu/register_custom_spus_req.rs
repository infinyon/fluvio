//!
//! # Create Custom Spus Request
//!
//! Converts Custom Spu API request into KV request and sends to KV store for processing.
//!
use tracing::{debug, trace};
use std::io::Error as IoError;

use dataplane::ErrorCode;
use fluvio_controlplane_metadata::spu::store::SpuLocalStorePolicy;
use fluvio_sc_schema::Status;
use fluvio_sc_schema::spu::CustomSpuSpec;
use fluvio_sc_schema::spu::SpuSpec;
use crate::core::*;

pub struct RegisterCustomSpu {
    ctx: SharedContext,
    name: String,
    spec: CustomSpuSpec,
}

impl RegisterCustomSpu {
    /// Handler for create spus request
    pub async fn handle_register_custom_spu_request(
        name: String,
        spec: CustomSpuSpec,
        dry_run: bool,
        ctx: SharedContext,
    ) -> Status {
        debug!("api request: create custom-spu '{}({})'", name, spec.id);

        let cmd = Self { name, spec, ctx };

        // validate custom-spu request
        if let Err(status) = cmd.validate_custom_spu_request().await {
            debug!("custom validation failed: {:?}", status);
            return status;
        }

        if dry_run {
            return Status::default();
        }

        let status = cmd.process_custom_spu_request().await;

        trace!("create custom-spus response {:#?}", status);

        status
    }

    /// Validate custom_spu requests (one at a time)
    async fn validate_custom_spu_request(&self) -> Result<(), Status> {
        debug!("validating custom-spu: {}({})", self.name, self.spec.id);

        // look-up SPU by name or id to check if already exists
        if self.ctx.spus().store().value(&self.name).await.is_some()
            || self
                .ctx
                .spus()
                .store()
                .get_by_id(self.spec.id)
                .await
                .is_some()
        {
            Err(Status::new(
                self.name.to_owned(),
                ErrorCode::SpuAlreadyExists,
                Some(format!(
                    "spu '{}({})' already defined",
                    self.name, self.spec.id
                )),
            ))
        } else {
            Ok(())
        }
    }

    /// Process custom spu, converts spu spec to K8 and sends to KV store
    async fn process_custom_spu_request(&self) -> Status {
        if let Err(err) = self.register_custom_spu().await {
            let error = Some(err.to_string());
            Status::new(self.name.to_owned(), ErrorCode::SpuError, error)
        } else {
            Status::new_ok(self.name.to_owned())
        }
    }

    /// register custom spu by convert into spu spec since custom spec is just subset
    async fn register_custom_spu(&self) -> Result<(), IoError> {
        let spu_spec: SpuSpec = self.spec.clone().into();

        self.ctx
            .spus()
            .create_spec(self.name.to_owned(), spu_spec)
            .await
            .map(|_| ())
    }
}

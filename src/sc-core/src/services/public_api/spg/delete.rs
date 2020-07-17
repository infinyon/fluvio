use log::{debug};
use std::io::Error;

use sc_api::FlvStatus;

use crate::core::*;

/// Handler for delete spu group request
pub async fn handle_delete_spu_group(name: String, ctx: SharedContext) -> Result<FlvStatus, Error> {
    use flv_metadata::spg::K8SpuGroupSpec;

    debug!("delete spg group: {}", name);

    /*
    let status = match ctx.delete::<K8SpuGroupSpec>(&name).await {
        Ok(_) => FlvStatus::new_ok(name),
        Err(err) => {
              let error = Some(err.to_string());
              FlvStatus::new(name, FlvErrorCode::SpuError, error)
        }
    };

    trace!("flv delete spu group resp {:#?}", status);

    Ok(status)
    */
    unimplemented!()
}

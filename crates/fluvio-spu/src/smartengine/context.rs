use fluvio_smartengine::{SmartModuleChainInstance, Version, Lookback};
use fluvio_protocol::{link::ErrorCode, Decoder};
use fluvio_smartmodule::Record;
use fluvio_spu_schema::server::smartmodule::{SmartModuleInvocation, SmartModuleInvocationWasm};
use tracing::{debug, trace};

use crate::{
    core::DefaultSharedGlobalContext, replication::leader::SharedFileLeaderState,
    smartengine::file_batch::FileBatchIterator,
};
use crate::smartengine::chain;

pub struct SmartModuleContext {
    chain: SmartModuleChainInstance,
    version: Version,
}

impl SmartModuleContext {
    pub async fn try_from(
        smartmodule: Vec<SmartModuleInvocation>,
        version: i16,
        ctx: &DefaultSharedGlobalContext,
    ) -> Result<Option<Self>, ErrorCode> {
        Self::build_smartmodule_context(smartmodule, version, ctx).await
    }

    pub fn chain_mut(&mut self) -> &mut SmartModuleChainInstance {
        &mut self.chain
    }

    pub async fn look_back(&mut self, replica: &SharedFileLeaderState) -> Result<(), ErrorCode> {
        self.chain
            .look_back(|lookback| read_records(replica, lookback, self.version))
            .await
            .map_err(|err| {
                ErrorCode::SmartModuleLookBackError(format!("error in look_back chain: {err}"))
            })
    }

    /// given SmartModule invocation and context, generate execution context
    async fn build_smartmodule_context(
        invocations: Vec<SmartModuleInvocation>,
        version: Version,
        ctx: &DefaultSharedGlobalContext,
    ) -> Result<Option<Self>, ErrorCode> {
        if invocations.is_empty() {
            return Ok(None);
        }

        let mut fetched_invocations = Vec::with_capacity(invocations.len());
        for invocation in invocations {
            fetched_invocations.push(resolve_invocation(invocation, ctx)?)
        }

        Ok(Some(Self {
            chain: chain::build_chain(fetched_invocations, version, ctx.smartengine_owned())?,
            version,
        }))
    }
}

fn resolve_invocation(
    invocation: SmartModuleInvocation,
    ctx: &DefaultSharedGlobalContext,
) -> Result<SmartModuleInvocation, ErrorCode> {
    if let SmartModuleInvocationWasm::Predefined(name) = invocation.wasm {
        if let Some(smartmodule) = ctx
            .smartmodule_localstore()
            .find_by_pk_key(&name)
            .map_err(|err| ErrorCode::Other(format!("error parsing SmartModule name: {err}")))?
        {
            Ok(SmartModuleInvocation {
                wasm: SmartModuleInvocationWasm::AdHoc(smartmodule.spec.wasm.payload.into()),
                ..invocation
            })
        } else {
            Err(ErrorCode::SmartModuleNotFound { name })
        }
    } else {
        Ok(invocation)
    }
}

async fn read_records(
    replica: &SharedFileLeaderState,
    lookback: Lookback,
    version: Version,
) -> anyhow::Result<Vec<Record>> {
    let (start_offset, hw) = replica.start_offset_info().await;

    let (offset, count) = match lookback {
        Lookback::Last(last) => (
            (hw - (TryInto::<i64>::try_into(last)?)).max(0),
            last.try_into()?,
        ),
    };
    let offset = offset.clamp(start_offset, hw);
    debug!(offset, ?lookback, "reading records for look_back");

    let slice = replica
        .read_records(offset, u32::MAX, fluvio::Isolation::ReadCommitted)
        .await?;

    let Some(file_slice) = slice.file_slice else {
        trace!(?slice);
        debug!("read 0 records");
        return Ok(Default::default())
    };

    let file_batch_iterator = FileBatchIterator::from_raw_slice(file_slice);

    let mut result = Vec::with_capacity(count);
    for batch_result in file_batch_iterator {
        let input_batch = batch_result?;

        let mut records: Vec<Record> = vec![];
        Decoder::decode(
            &mut records,
            &mut std::io::Cursor::new(input_batch.records),
            version,
        )?;
        result.append(&mut records);
    }
    let result: Vec<Record> = result.into_iter().rev().take(count).rev().collect();
    debug!("read {} records", result.len());
    trace!(?result);
    Ok(result)
}

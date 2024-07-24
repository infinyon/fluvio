use std::sync::Arc;
use std::time::Duration;

use async_lock::RwLock;
use chrono::Utc;
use fluvio_protocol::link::ErrorCode;
use fluvio_smartmodule::Record;
use fluvio_spu_schema::server::smartmodule::{SmartModuleInvocation, SmartModuleInvocationWasm};
use fluvio_storage::ReplicaStorage;
use fluvio_storage::iterators::{FileBatch, FileBatchIterator, FileRecordIterator, RecordItem};
use fluvio_types::Timestamp;
use tracing::{debug, trace, error};

use crate::core::GlobalContext;
use crate::core::metrics::SpuMetrics;
use crate::replication::leader::LeaderReplicaState;

use crate::smartengine::chain;
use crate::smartengine::Lookback;
use crate::smartengine::SmartModuleChainBuilder;
use crate::smartengine::SmartModuleChainInstance;
use crate::smartengine::Version;

#[derive(Debug)]
pub struct SmartModuleContext {
    chain: SmartModuleChainInstance,
    version: Version,
    spu_metrics: Arc<SpuMetrics>,
}

pub type SharedSmartModuleContext = Arc<RwLock<SmartModuleContext>>;

impl SmartModuleContext {
    pub async fn try_from<R: ReplicaStorage>(
        smartmodule: Vec<SmartModuleInvocation>,
        version: i16,
        ctx: &GlobalContext<R>,
    ) -> Result<Option<Self>, ErrorCode> {
        Self::build_smartmodule_context(smartmodule, version, ctx).await
    }

    pub fn chain_mut(&mut self) -> &mut SmartModuleChainInstance {
        &mut self.chain
    }

    pub async fn look_back<R: ReplicaStorage>(
        &mut self,
        replica: &LeaderReplicaState<R>,
    ) -> Result<(), ErrorCode> {
        self.chain
            .look_back(
                |lookback| read_records(replica, lookback, self.version),
                self.spu_metrics.chain_metrics(),
            )
            .await
            .map_err(|err| {
                error!("look_back chain error: {err:#}");
                ErrorCode::SmartModuleLookBackError(err.root_cause().to_string())
            })
    }

    /// given SmartModule invocation and context, generate execution context
    async fn build_smartmodule_context<R: ReplicaStorage>(
        invocations: Vec<SmartModuleInvocation>,
        version: Version,
        ctx: &GlobalContext<R>,
    ) -> Result<Option<Self>, ErrorCode> {
        if invocations.is_empty() {
            return Ok(None);
        }

        let mut fetched_invocations = Vec::with_capacity(invocations.len());
        for invocation in invocations {
            fetched_invocations.push(resolve_invocation(invocation, ctx)?)
        }
        let mut chain_builder = SmartModuleChainBuilder::default();
        chain_builder.set_store_memory_limit(ctx.config().smart_engine.store_max_memory);

        let chain = chain::build_chain(
            chain_builder,
            fetched_invocations,
            version,
            ctx.smartengine_owned(),
        )?;

        Ok(Some(Self {
            chain,
            version,
            spu_metrics: ctx.metrics(),
        }))
    }
}

fn resolve_invocation<R: ReplicaStorage>(
    invocation: SmartModuleInvocation,
    ctx: &GlobalContext<R>,
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

async fn read_records<R: ReplicaStorage>(
    replica: &LeaderReplicaState<R>,
    lookback: Lookback,
    version: Version,
) -> anyhow::Result<Vec<Record>> {
    let iter = lookback_iterator(replica, lookback, version).await?;

    let result: Vec<Record> = iter.collect::<Result<Vec<Record>, std::io::Error>>()?;
    debug!("read {} records", result.len());
    trace!(?result);
    Ok(result)
}

async fn lookback_iterator<R: ReplicaStorage>(
    replica: &LeaderReplicaState<R>,
    lookback: Lookback,
    version: Version,
) -> anyhow::Result<Box<dyn Iterator<Item = Result<Record, std::io::Error>>>> {
    let iter = match lookback {
        Lookback::Last(last) => lookback_last_iterator(replica, last, version).await,
        Lookback::Age { age, last } => lookback_age_iterator(replica, age, last, version).await,
    }?;
    let iter = iter.map(|it| it.map(|res| res.record));
    Ok(Box::new(iter))
}

async fn lookback_last_iterator<R: ReplicaStorage>(
    replica: &LeaderReplicaState<R>,
    last: u64,
    version: Version,
) -> anyhow::Result<Box<dyn Iterator<Item = Result<RecordItem, std::io::Error>>>> {
    let (start_offset, hw) = replica.start_offset_info().await;

    let offset = (hw - (TryInto::<i64>::try_into(last)?)).max(0);

    let offset = offset.clamp(start_offset, hw);
    debug!(offset, "reading last {last} records for look_back");

    let slice = replica
        .read_records(offset, u32::MAX, fluvio::Isolation::ReadCommitted)
        .await?;

    let Some(file_slice) = slice.file_slice else {
        trace!(?slice);
        return Ok(Box::new(std::iter::empty()));
    };

    let batch_iter = FileBatchIterator::from_raw_slice(file_slice);
    let records_iter = FileRecordIterator::new(batch_iter, version);

    Ok(Box::new(records_iter.filter(move |r| match r {
        Ok(item) => item.offset >= offset,
        Err(_) => true,
    })))
}

async fn lookback_age_iterator<R: ReplicaStorage>(
    replica: &LeaderReplicaState<R>,
    age: Duration,
    last: u64,
    version: Version,
) -> anyhow::Result<Box<dyn Iterator<Item = Result<RecordItem, std::io::Error>>>> {
    let min_timestamp: Timestamp = Utc::now()
        .timestamp_millis()
        .checked_sub(i64::try_from(age.as_millis())?)
        .ok_or_else(|| anyhow::anyhow!("timestamp overflow"))?;

    debug!(?age, last, min_timestamp, "iterating for lookback");

    let records_iter = if last > 0 {
        lookback_last_iterator(replica, last, version).await?
    } else {
        let batches = read_batches_by_age(replica, min_timestamp).await?;
        Box::new(FileRecordIterator::new(
            batches.into_iter().map(Ok),
            version,
        ))
    };
    Ok(Box::new(records_iter.filter(move |i| match i {
        Ok(item) => item.timestamp >= min_timestamp,
        Err(_) => true,
    })))
}

async fn read_batches_by_age<R: ReplicaStorage>(
    replica: &LeaderReplicaState<R>,
    min_timestamp: Timestamp,
) -> anyhow::Result<Vec<FileBatch>> {
    let mut result = Vec::new();
    let mut offset = replica.hw() - 1;
    loop {
        if offset.is_negative() {
            break;
        }
        trace!(offset, "reading next batch");
        let slice = replica
            .read_records(offset - 1, u32::MAX, fluvio::Isolation::ReadCommitted)
            .await?;
        let Some(file_slice) = slice.file_slice else {
            trace!(?slice);
            break;
        };
        let mut batch_iter = FileBatchIterator::from_raw_slice(file_slice);
        let Some(batch) = batch_iter.next() else {
            break;
        };
        let batch = batch?;
        trace!(?batch.batch, "next file batch");

        if batch.batch.header.max_time_stamp < min_timestamp {
            break;
        } else {
            trace!(offset, "added batch");
            offset = batch.batch.base_offset - 1;
            result.push(batch);
        }
    }
    result.reverse();
    debug!(
        min_timestamp,
        "read {} batches older than min_timestamp",
        result.len()
    );
    Ok(result)
}

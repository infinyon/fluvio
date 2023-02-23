use fluvio_smartengine::SmartModuleChainInstance;
use tracing::{error};

use fluvio_protocol::link::ErrorCode;
use fluvio_spu_schema::server::{
    smartmodule::{
        SmartModuleInvocation, SmartModuleKind, SmartModuleContextData, SmartModuleInvocationWasm,
    },
};
use futures_util::{StreamExt, stream::BoxStream};
use fluvio_protocol::record::ConsumerRecord;

use crate::core::DefaultSharedGlobalContext;
use crate::smartengine::chain;

pub struct SmartModuleContext {
    pub chain: SmartModuleChainInstance,
    pub right_consumer_stream:
        Option<BoxStream<'static, Result<fluvio::consumer::Record, ErrorCode>>>,
}

impl SmartModuleContext {
    pub async fn try_from(
        smartmodule: Vec<SmartModuleInvocation>,
        version: i16,
        ctx: &DefaultSharedGlobalContext,
    ) -> Result<Option<Self>, ErrorCode> {
        Self::build_smartmodule_context(smartmodule, version, ctx).await
    }

    /// given SmartModule invocation and context, generate execution context
    async fn build_smartmodule_context(
        invocations: Vec<SmartModuleInvocation>,
        version: i16,
        ctx: &DefaultSharedGlobalContext,
    ) -> Result<Option<Self>, ErrorCode> {
        if invocations.is_empty() {
            return Ok(None);
        }
        // check for right consumer stream exists, this only happens for join type
        let mut right_consumer_stream = None;
        let mut fetched_invocations = Vec::with_capacity(invocations.len());
        for invocation in invocations {
            let next_right_stream = extract_right_stream(&invocation, ctx).await?;
            match (&mut right_consumer_stream, next_right_stream) {
                (Some(_), Some(_)) => return Err(ErrorCode::DerivedStreamAlreadyExists),
                (Some(_), None) => {}
                (current, next) => *current = next,
            }
            fetched_invocations.push(resolve_invocation(invocation, ctx)?)
        }

        Ok(Some(Self {
            chain: chain::build_chain(fetched_invocations, version, ctx.smartengine_owned())?,
            right_consumer_stream,
        }))
    }
}

async fn extract_right_stream<'a, 'b>(
    invocation: &'a SmartModuleInvocation,
    ctx: &'b DefaultSharedGlobalContext,
) -> Result<Option<BoxStream<'static, Result<ConsumerRecord, ErrorCode>>>, ErrorCode> {
    let right_consumer_stream = match invocation.kind {
        // for join, create consumer stream
        SmartModuleKind::Join(ref topic)
        | SmartModuleKind::Generic(SmartModuleContextData::Join(ref topic)) => {
            let consumer = ctx.leaders().partition_consumer(topic.to_owned(), 0).await;

            Some(
                consumer
                    .stream(fluvio::Offset::beginning())
                    .await
                    .map_err(|err| {
                        error!("error fetching join data {}", err);
                        ErrorCode::DerivedStreamJoinFetchError
                    })?
                    .boxed(),
            )
        }
        SmartModuleKind::JoinStream {
            topic: ref _topic,
            derivedstream: ref derivedstream_name,
        }
        | SmartModuleKind::Generic(SmartModuleContextData::JoinStream {
            topic: ref _topic,
            derivedstream: ref derivedstream_name,
        }) => {
            // first ensure derivedstream exists

            return Err(ErrorCode::DerivedStreamNotFound(
                derivedstream_name.to_owned(),
            ));
        }
        _ => None,
    };
    Ok(right_consumer_stream)
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

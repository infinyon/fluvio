use fluvio_smartengine::SmartModuleChainInstance;
use tracing::{debug, error};

use fluvio_controlplane_metadata::derivedstream::{DerivedStreamInputRef, DerivedStreamStep};
use fluvio_protocol::link::ErrorCode;
use fluvio_spu_schema::server::{
    stream_fetch::DerivedStreamInvocation,
    smartmodule::{
        SmartModuleInvocation, SmartModuleKind, SmartModuleContextData, SmartModuleInvocationWasm,
    },
};
use fluvio::{ConsumerConfig};
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
        derivedstream: Option<DerivedStreamInvocation>,
        version: i16,
        ctx: &DefaultSharedGlobalContext,
    ) -> Result<Option<Self>, ErrorCode> {
        match derivedstream {
            Some(ss_inv) => {
                let derived_stream_invocation = derivedstream_to_invocation(ss_inv, ctx).await?;
                Self::build_smartmodule_context(vec![derived_stream_invocation], version, ctx).await
            }
            None => Self::build_smartmodule_context(smartmodule, version, ctx).await,
        }
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

async fn derivedstream_to_invocation(
    invocation: DerivedStreamInvocation,
    ctx: &DefaultSharedGlobalContext,
) -> Result<SmartModuleInvocation, ErrorCode> {
    let name = invocation.stream;
    debug!(%name,"extracting derivedstream");
    let params = invocation.params;
    let ss_list = ctx.derivedstream_store().all_keys();
    debug!("derivedstreams: {:#?}", ss_list);
    if let Some(derivedstream) = ctx.derivedstream_store().spec(&name) {
        let spec = derivedstream.spec;
        if derivedstream.valid {
            let mut steps = spec.steps.steps;
            if steps.is_empty() {
                debug!(name = %name,"no steps in derivedstream");
                Err(ErrorCode::DerivedStreamInvalid(name))
            } else {
                // for now, only perform a single step
                let step = steps.pop().expect("first one");
                let sm = match step {
                    DerivedStreamStep::Aggregate(module) => SmartModuleInvocation {
                        wasm: SmartModuleInvocationWasm::Predefined(module.module),
                        kind: SmartModuleKind::Aggregate {
                            accumulator: vec![],
                        },
                        params,
                    },
                    DerivedStreamStep::Map(module) => SmartModuleInvocation {
                        wasm: SmartModuleInvocationWasm::Predefined(module.module),
                        kind: SmartModuleKind::Map,
                        params,
                    },
                    DerivedStreamStep::FilterMap(module) => SmartModuleInvocation {
                        wasm: SmartModuleInvocationWasm::Predefined(module.module),
                        kind: SmartModuleKind::FilterMap,
                        params,
                    },
                    DerivedStreamStep::Filter(module) => SmartModuleInvocation {
                        wasm: SmartModuleInvocationWasm::Predefined(module.module),
                        kind: SmartModuleKind::Filter,
                        params,
                    },
                    DerivedStreamStep::Join(module) => match module.right {
                        DerivedStreamInputRef::Topic(ref topic) => SmartModuleInvocation {
                            wasm: SmartModuleInvocationWasm::Predefined(module.module),
                            kind: SmartModuleKind::Join(topic.name.to_owned()),
                            params,
                        },
                        DerivedStreamInputRef::DerivedStream(ref smart_stream) => {
                            let join_target_name = smart_stream.name.to_owned();
                            // ensure derivedstream exists
                            if let Some(ctx) = ctx.derivedstream_store().spec(&join_target_name) {
                                let target_input = ctx.spec.input;
                                // check target input, we can only do 1 level recursive definition now.
                                match target_input {
                                    DerivedStreamInputRef::Topic(topic_target) => {
                                        SmartModuleInvocation {
                                            wasm: SmartModuleInvocationWasm::Predefined(
                                                module.module,
                                            ),
                                            kind: SmartModuleKind::JoinStream {
                                                topic: topic_target.name,
                                                derivedstream: join_target_name.to_owned(),
                                            },
                                            params,
                                        }
                                    }

                                    DerivedStreamInputRef::DerivedStream(child_child_target) => {
                                        return Err(ErrorCode::DerivedStreamRecursion(
                                            join_target_name,
                                            child_child_target.name,
                                        ));
                                    }
                                }
                            } else {
                                return Err(ErrorCode::DerivedStreamNotFound(join_target_name));
                            }
                        }
                    },
                };

                Ok(sm)
            }
        } else {
            debug!(%name, "invalid DerivedStream");
            Err(ErrorCode::DerivedStreamInvalid(name))
        }
    } else {
        Err(ErrorCode::DerivedStreamNotFound(name))
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
            if let Some(derivedstream) = ctx.derivedstream_store().spec(derivedstream_name) {
                // find input which has topic
                match derivedstream.spec.input {
                    DerivedStreamInputRef::Topic(topic) => {
                        let consumer = ctx
                            .leaders()
                            .partition_consumer(topic.name.to_owned(), 0)
                            .await;
                        // need to build stream arg
                        let mut builder = ConsumerConfig::builder();

                        builder.derivedstream(Some(DerivedStreamInvocation {
                            stream: derivedstream_name.to_owned(),
                            params: invocation.params.clone(),
                        }));

                        let consume_config = builder.build().map_err(|err| {
                            error!("error building consumer config {}", err);
                            ErrorCode::Other(format!("error building consumer config {err}"))
                        })?;
                        Some(
                            consumer
                                .stream_with_config(fluvio::Offset::beginning(), consume_config)
                                .await
                                .map_err(|err| {
                                    error!("error fetching join data {}", err);
                                    ErrorCode::DerivedStreamJoinFetchError
                                })?
                                .boxed(),
                        )
                    }
                    DerivedStreamInputRef::DerivedStream(child_smart) => {
                        return Err(ErrorCode::DerivedStreamRecursion(
                            derivedstream_name.to_owned(),
                            child_smart.name,
                        ));
                    }
                }
            } else {
                return Err(ErrorCode::DerivedStreamNotFound(
                    derivedstream_name.to_owned(),
                ));
            }
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

use fluvio_controlplane_metadata::smartstream::{SmartStreamInputRef, SmartStreamStep};
use tracing::{debug, error};

use dataplane::{ErrorCode, smartmodule::SmartModuleRuntimeError};
use fluvio::{
    ConsumerConfig,
    consumer::{SmartModuleInvocation, SmartStreamInvocation, SmartModuleKind},
};
use fluvio_smartengine::SmartModuleInstance;
use fluvio_spu_schema::server::stream_fetch::{
    SmartModuleInvocationWasm, SmartModulePayload, SmartModuleWasm,
};
use futures_util::{StreamExt, stream::BoxStream};

use crate::core::DefaultSharedGlobalContext;

pub struct SmartStreamContext {
    pub smartstream: Box<dyn SmartModuleInstance>,
    pub right_consumer_stream:
        Option<BoxStream<'static, Result<fluvio::consumer::Record, ErrorCode>>>,
}

impl SmartStreamContext {
    /// find wasm payload, they can be loaded from payload or from smart module
    /// smart module has precedent over payload
    pub async fn extract(
        wasm_payload: Option<SmartModulePayload>,
        smartmodule: Option<SmartModuleInvocation>,
        smart_stream: Option<SmartStreamInvocation>,
        ctx: &DefaultSharedGlobalContext,
    ) -> Result<Option<Self>, ErrorCode> {
        let derived_sm_modules = if let Some(ss_inv) = smart_stream {
            Some(extract_smartstream_context(ss_inv, ctx).await?)
        } else {
            None
        };

        // if module is come from smart stream, then we can use it
        let module = if let Some(derive) = derived_sm_modules {
            Some(derive)
        } else {
            smartmodule
        };

        match module {
            Some(smartmodule_invocation) => Ok(Some(
                Self::extract_smartmodule_context(smartmodule_invocation, ctx).await?,
            )),
            None => {
                if let Some(payload) = wasm_payload {
                    Ok(Some(Self {
                        smartstream: Self::payload_to_smartmodule(payload, ctx)?,
                        right_consumer_stream: None,
                    }))
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// given smartstream invocation and context, generate execution context
    async fn extract_smartmodule_context(
        invocation: SmartModuleInvocation,
        ctx: &DefaultSharedGlobalContext,
    ) -> Result<Self, ErrorCode> {
        // check for right consumer stream exists, this only happens for join type
        let right_consumer_stream = match invocation.kind {
            // for join, create consumer stream
            SmartModuleKind::Join(ref topic) => {
                let consumer = ctx.leaders().partition_consumer(topic.to_owned(), 0).await;

                Some(
                    consumer
                        .stream(fluvio::Offset::beginning())
                        .await
                        .map_err(|err| {
                            error!("error fetching join data {}", err);
                            ErrorCode::SmartStreamJoinFetchError
                        })?
                        .boxed(),
                )
            }
            SmartModuleKind::JoinStream {
                topic: ref _topic,
                smartstream: ref smartstream_name,
            } => {
                // first ensure smartstream exists
                if let Some(smartstream) = ctx.smartstream_store().spec(smartstream_name) {
                    // find input which has topic
                    match smartstream.spec.input {
                        SmartStreamInputRef::Topic(topic) => {
                            let consumer = ctx
                                .leaders()
                                .partition_consumer(topic.name.to_owned(), 0)
                                .await;
                            // need to build stream arg
                            let mut builder = ConsumerConfig::builder();

                            builder.smartstream(Some(SmartStreamInvocation {
                                stream: smartstream_name.to_owned(),
                                params: invocation.params.clone(),
                            }));

                            let consume_config = builder.build().map_err(|err| {
                                error!("error building consumer config {}", err);
                                ErrorCode::Other(format!("error building consumer config {}", err))
                            })?;
                            Some(
                                consumer
                                    .stream_with_config(fluvio::Offset::beginning(), consume_config)
                                    .await
                                    .map_err(|err| {
                                        error!("error fetching join data {}", err);
                                        ErrorCode::SmartStreamJoinFetchError
                                    })?
                                    .boxed(),
                            )
                        }
                        SmartStreamInputRef::SmartStream(child_smart) => {
                            return Err(ErrorCode::SmartStreamRecursion(smartstream_name.to_owned(), child_smart.name));
                        }
                    }
                } else {
                    return Err(ErrorCode::SmartStreamNotFound(smartstream_name.to_owned()));
                }
            }
            _ => None,
        };

        // then get smartstream context
        let payload = match invocation.wasm {
            SmartModuleInvocationWasm::Predefined(name) => {
                if let Some(smartmodule) = ctx.smartmodule_localstore().spec(&name) {
                    let wasm = SmartModuleWasm::Gzip(smartmodule.wasm.payload);
                    SmartModulePayload {
                        wasm,
                        kind: invocation.kind,
                        params: invocation.params,
                    }
                } else {
                    return Err(ErrorCode::SmartModuleNotFound { name });
                }
            }
            SmartModuleInvocationWasm::AdHoc(bytes) => {
                let wasm = SmartModuleWasm::Gzip(bytes);
                SmartModulePayload {
                    wasm,
                    kind: invocation.kind,
                    params: invocation.params,
                }
            }
        };

        Ok(Self {
            smartstream: Self::payload_to_smartmodule(payload, ctx)?,
            right_consumer_stream,
        })
    }

    fn payload_to_smartmodule(
        payload: SmartModulePayload,
        ctx: &DefaultSharedGlobalContext,
    ) -> Result<Box<dyn SmartModuleInstance>, ErrorCode> {
        let raw = payload.wasm.get_raw().map_err(|err| {
            ErrorCode::SmartModuleInvalid { error: err.to_string(), name: None }
        })?;

        debug!(len = raw.len(), "wasm WASM module with bytes");

        let sm_engine = ctx.smartstream_owned();
        let kind = payload.kind.clone();

        sm_engine
            .create_module_from_payload(payload)
            .map_err(|err| {
                error!(
                    error = err.to_string().as_str(),
                    "Error Instantiating SmartStream"
                );
                ErrorCode::SmartModuleInvalidExports { kind: format!("{:?}", kind), error: err.to_string() }
            })
    }
}

async fn extract_smartstream_context(
    invocation: SmartStreamInvocation,
    ctx: &DefaultSharedGlobalContext,
) -> Result<SmartModuleInvocation, ErrorCode> {
    let name = invocation.stream;
    debug!(%name,"extracting smartstream");
    let params = invocation.params;
    let ss_list = ctx.smartstream_store().all_keys();
    debug!("smartstreams: {:#?}", ss_list);
    if let Some(smartstream) = ctx.smartstream_store().spec(&name) {
        let spec = smartstream.spec;
        if smartstream.valid {
            let mut steps = spec.steps.steps;
            if steps.is_empty() {
                debug!(name = %name,"no steps in smartstream");
                Err(ErrorCode::SmartStreamInvalid(name))
            } else {
                // for now, only perform a single step
                let step = steps.pop().expect("first one");
                let sm = match step {
                    SmartStreamStep::Aggregate(module) => SmartModuleInvocation {
                        wasm: SmartModuleInvocationWasm::Predefined(module.module),
                        kind: SmartModuleKind::Aggregate {
                            accumulator: vec![],
                        },
                        params,
                    },
                    SmartStreamStep::Map(module) => SmartModuleInvocation {
                        wasm: SmartModuleInvocationWasm::Predefined(module.module),
                        kind: SmartModuleKind::Map,
                        params,
                    },
                    SmartStreamStep::FilterMap(module) => SmartModuleInvocation {
                        wasm: SmartModuleInvocationWasm::Predefined(module.module),
                        kind: SmartModuleKind::FilterMap,
                        params,
                    },
                    SmartStreamStep::Filter(module) => SmartModuleInvocation {
                        wasm: SmartModuleInvocationWasm::Predefined(module.module),
                        kind: SmartModuleKind::Filter,
                        params,
                    },
                    SmartStreamStep::Join(module) => match module.right {
                        SmartStreamInputRef::Topic(ref topic) => SmartModuleInvocation {
                            wasm: SmartModuleInvocationWasm::Predefined(module.module),
                            kind: SmartModuleKind::Join(topic.name.to_owned()),
                            params,
                        },
                        SmartStreamInputRef::SmartStream(ref smart_stream) => {
                            let join_target_name = smart_stream.name.to_owned();
                            // ensure smartstream exists
                            if let Some(ctx) = ctx.smartstream_store().spec(&join_target_name) {
                                let target_input = ctx.spec.input;
                                // check target input, we can only do 1 level recursive definition now.
                                match target_input {
                                    SmartStreamInputRef::Topic(topic_target) => {
                                        SmartModuleInvocation {
                                            wasm: SmartModuleInvocationWasm::Predefined(
                                                module.module,
                                            ),
                                            kind: SmartModuleKind::JoinStream {
                                                topic: topic_target.name,
                                                smartstream: join_target_name.to_owned(),
                                            },
                                            params,
                                        }
                                    }

                                    SmartStreamInputRef::SmartStream(child_child_target) => {
                                        return Err(ErrorCode::SmartStreamRecursion(join_target_name, child_child_target.name));
                                    }
                                }
                            } else {
                                return Err(ErrorCode::SmartStreamNotFound(join_target_name));
                            }
                        }
                    },
                };

                Ok(sm)
            }
        } else {
            debug!(%name, "invalid smart stream");
            Err(ErrorCode::SmartStreamInvalid(name))
        }
    } else {
        Err(ErrorCode::SmartStreamNotFound(name))
    }
}

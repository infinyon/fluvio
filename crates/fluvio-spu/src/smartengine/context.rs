use fluvio_smartengine::{SmartModuleConfig, SmartModuleInitialData, SmartModuleChainInstance};
use tracing::{debug, error};

use fluvio_controlplane_metadata::derivedstream::{DerivedStreamInputRef, DerivedStreamStep};
use fluvio_protocol::link::ErrorCode;
use fluvio_spu_schema::server::{
    stream_fetch::DerivedStreamInvocation,
    smartmodule::{
        LegacySmartModulePayload, SmartModuleInvocation, SmartModuleKind, SmartModuleContextData,
        SmartModuleInvocationWasm, SmartModuleWasmCompressed,
    },
};
use fluvio::{ConsumerConfig};
use futures_util::{StreamExt, stream::BoxStream};

use crate::core::DefaultSharedGlobalContext;

pub struct SmartModuleContext {
    pub chain: SmartModuleChainInstance,
    pub right_consumer_stream:
        Option<BoxStream<'static, Result<fluvio::consumer::Record, ErrorCode>>>,
}

impl SmartModuleContext {
    /// find wasm payload, they can be loaded from payload or from smart module
    /// smart module has precedent over payload
    pub async fn extract(
        wasm_payload: Option<LegacySmartModulePayload>,
        smartmodule: Option<SmartModuleInvocation>,
        derivedstream: Option<DerivedStreamInvocation>,
        version: i16,
        ctx: &DefaultSharedGlobalContext,
    ) -> Result<Option<Self>, ErrorCode> {
        let derived_sm_modules = if let Some(ss_inv) = derivedstream {
            Some(extract_derivedstream_context(ss_inv, ctx).await?)
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
                Self::extract_smartmodule_context(smartmodule_invocation, version, ctx).await?,
            )),
            None => {
                if let Some(payload) = wasm_payload {
                    Ok(Some(Self {
                        chain: Self::payload_to_smartmodule(payload, version, ctx)?,
                        right_consumer_stream: None,
                    }))
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// given SmartModule invocation and context, generate execution context
    async fn extract_smartmodule_context(
        invocation: SmartModuleInvocation,
        version: i16,
        ctx: &DefaultSharedGlobalContext,
    ) -> Result<Self, ErrorCode> {
        // check for right consumer stream exists, this only happens for join type
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
                                ErrorCode::Other(format!("error building consumer config {}", err))
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

        // then get smartmodule context
        let payload = match invocation.wasm {
            SmartModuleInvocationWasm::Predefined(name) => {
                if let Some(smartmodule) = ctx
                    .smartmodule_localstore()
                    .find_by_pk_key(&name)
                    .map_err(|err| {
                        ErrorCode::Other(format!("error parsing SmartModule name: {}", err))
                    })?
                {
                    let wasm = SmartModuleWasmCompressed::Gzip(smartmodule.spec.wasm.payload);
                    LegacySmartModulePayload {
                        wasm,
                        kind: invocation.kind,
                        params: invocation.params,
                    }
                } else {
                    return Err(ErrorCode::SmartModuleNotFound { name });
                }
            }
            SmartModuleInvocationWasm::AdHoc(bytes) => {
                let wasm = SmartModuleWasmCompressed::Gzip(bytes);
                LegacySmartModulePayload {
                    wasm,
                    kind: invocation.kind,
                    params: invocation.params,
                }
            }
        };

        Ok(Self {
            chain: Self::payload_to_smartmodule(payload, version, ctx)?,
            right_consumer_stream,
        })
    }

    fn payload_to_smartmodule(
        payload: LegacySmartModulePayload,
        version: i16,
        ctx: &DefaultSharedGlobalContext,
    ) -> Result<SmartModuleChainInstance, ErrorCode> {
        let raw = payload
            .wasm
            .get_raw()
            .map_err(|err| ErrorCode::SmartModuleInvalid {
                error: err.to_string(),
                name: None,
            })?;

        debug!(len = raw.len(), "SmartModule with bytes");

        let sm_engine = ctx.smartengine_owned();
        let mut chain_builder = sm_engine.builder();

        let kind = payload.kind.clone();

        let initial_data = match kind {
            SmartModuleKind::Aggregate { ref accumulator } => {
                SmartModuleInitialData::with_aggregate(accumulator.clone())
            }
            SmartModuleKind::Generic(SmartModuleContextData::Aggregate { ref accumulator }) => {
                SmartModuleInitialData::with_aggregate(accumulator.clone())
            }
            _ => SmartModuleInitialData::default(),
        };

        debug!("param: {:#?}", payload.params);
        chain_builder
            .add_smart_module(
                SmartModuleConfig::builder()
                    .params(payload.params)
                    .version(version)
                    .initial_data(initial_data)
                    .build()
                    .map_err(|err| ErrorCode::SmartModuleInvalid {
                        error: err.to_string(),
                        name: None,
                    })?,
                raw,
            )
            .map_err(|err| {
                error!(
                    error = err.to_string().as_str(),
                    "Error Instantiating SmartModule"
                );
                if let SmartModuleKind::Generic(_) = kind {
                    ErrorCode::SmartModuleInvalid {
                        error: err.to_string(),
                        name: None,
                    }
                } else {
                    ErrorCode::SmartModuleInvalidExports {
                        kind: format!("{}", kind),
                        error: err.to_string(),
                    }
                }
            })?;
        let chain = chain_builder.initialize().map_err(|err| {
            error!(
                error = err.to_string().as_str(),
                "Error Initializing SmartModule"
            );
            ErrorCode::SmartModuleChainInitError(err.to_string())
        })?;
        Ok(chain)
    }
}

async fn extract_derivedstream_context(
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

use fluvio_controlplane_metadata::smartstream::{SmartStreamInputRef, SmartStreamStep};
use tracing::{debug, error};

use dataplane::{ErrorCode, SmartStreamError};
use fluvio::consumer::{SmartModuleInvocation, SmartStreamInvocation, SmartStreamKind};
use fluvio_smartengine::{SmartStream};
use fluvio_spu_schema::server::stream_fetch::{
    SmartModuleInvocationWasm, SmartStreamPayload, SmartStreamWasm,
};
use futures_util::{StreamExt, stream::BoxStream};

use crate::core::DefaultSharedGlobalContext;

pub struct SmartStreamContext {
    pub smartstream: Box<dyn SmartStream>,
    pub right_consumer_stream:
        Option<BoxStream<'static, Result<fluvio::consumer::Record, ErrorCode>>>,
}

impl SmartStreamContext {
    /// find wasm payload, they can be loaded from payload or from smart module
    /// smart module has precedent over payload
    pub async fn extract(
        wasm_payload: Option<SmartStreamPayload>,
        smart_module: Option<SmartModuleInvocation>,
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
            smart_module
        };

        match module {
            Some(smart_module_invocation) => Ok(Some(
                Self::extract_smartmodule_context(smart_module_invocation, ctx).await?,
            )),
            None => {
                if let Some(payload) = wasm_payload {
                    Ok(Some(Self {
                        smartstream: Self::payload_to_smartstream(payload, ctx)?,
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
            SmartStreamKind::Join(ref topic) => {
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
            SmartStreamKind::JoinStream(ref smartstream_name) => {
                if let Some(smartstream) = ctx.smartstream_store().spec(smartstream_name) {
                    // find input which has topic
                    match smartstream.spec.input {
                        SmartStreamInputRef::Topic(topic) => {
                            let consumer = ctx
                                .leaders()
                                .partition_consumer(topic.name.to_owned(), 0)
                                .await;

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
                        SmartStreamInputRef::SmartStream(child_smart) => {
                            return Err(ErrorCode::SmartStreamError(
                                SmartStreamError::InvalidSmartStream(format!(
                                    "can't do recursive smartstream yet: {}->{}",
                                    smartstream_name, child_smart.name
                                )),
                            ));
                        }
                    }
                } else {
                    return Err(ErrorCode::SmartStreamError(
                        SmartStreamError::UndefinedSmartStream(format!(
                            "SmartStream {} not foundin join stream",
                            smartstream_name
                        )),
                    ));
                }
            }
            _ => None,
        };

        // then get smartstream context
        let payload = match invocation.wasm {
            SmartModuleInvocationWasm::Predefined(name) => {
                if let Some(smart_module) = ctx.smart_module_localstore().spec(&name) {
                    let wasm = SmartStreamWasm::Gzip(smart_module.wasm.payload);
                    SmartStreamPayload {
                        wasm,
                        kind: invocation.kind,
                        params: invocation.params,
                    }
                } else {
                    return Err(ErrorCode::SmartStreamError(
                        SmartStreamError::UndefinedSmartModule(name),
                    ));
                }
            }
            SmartModuleInvocationWasm::AdHoc(bytes) => {
                let wasm = SmartStreamWasm::Gzip(bytes);
                SmartStreamPayload {
                    wasm,
                    kind: invocation.kind,
                    params: invocation.params,
                }
            }
        };

        Ok(Self {
            smartstream: Self::payload_to_smartstream(payload, ctx)?,
            right_consumer_stream,
        })
    }

    fn payload_to_smartstream(
        payload: SmartStreamPayload,
        ctx: &DefaultSharedGlobalContext,
    ) -> Result<Box<dyn SmartStream>, ErrorCode> {
        let raw = payload.wasm.get_raw().map_err(|err| {
            ErrorCode::SmartStreamError(SmartStreamError::InvalidWasmModule(err.to_string()))
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
                ErrorCode::SmartStreamError(SmartStreamError::InvalidSmartStreamModule(
                    format!("{:?}", kind),
                    err.to_string(),
                ))
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
    if let Some(smart_module) = ctx.smartstream_store().spec(&name) {
        let spec = smart_module.spec;
        if smart_module.valid {
            let mut steps = spec.steps.steps;
            if steps.is_empty() {
                debug!(name = %name,"no steps in smartstream");
                Err(ErrorCode::SmartStreamError(
                    SmartStreamError::InvalidSmartStream(name),
                ))
            } else {
                // for now, only perform a single step
                let step = steps.pop().expect("first one");
                let sm = match step {
                    SmartStreamStep::Aggregate(module) => SmartModuleInvocation {
                        wasm: SmartModuleInvocationWasm::Predefined(module.module),
                        kind: SmartStreamKind::Aggregate {
                            accumulator: vec![],
                        },
                        params,
                    },
                    SmartStreamStep::Map(module) => SmartModuleInvocation {
                        wasm: SmartModuleInvocationWasm::Predefined(module.module),
                        kind: SmartStreamKind::Map,
                        params,
                    },
                    SmartStreamStep::FilterMap(module) => SmartModuleInvocation {
                        wasm: SmartModuleInvocationWasm::Predefined(module.module),
                        kind: SmartStreamKind::FilterMap,
                        params,
                    },
                    SmartStreamStep::Filter(module) => SmartModuleInvocation {
                        wasm: SmartModuleInvocationWasm::Predefined(module.module),
                        kind: SmartStreamKind::Filter,
                        params,
                    },
                    SmartStreamStep::Join(module) => match module.right {
                        SmartStreamInputRef::Topic(ref topic) => SmartModuleInvocation {
                            wasm: SmartModuleInvocationWasm::Predefined(module.module),
                            kind: SmartStreamKind::Join(topic.name.to_owned()),
                            params,
                        },
                        SmartStreamInputRef::SmartStream(ref smart_stream) => {
                            SmartModuleInvocation {
                                wasm: SmartModuleInvocationWasm::Predefined(module.module),
                                kind: SmartStreamKind::JoinStream(smart_stream.name.to_owned()),
                                params,
                            }
                        }
                    },
                };

                Ok(sm)
            }
        } else {
            debug!(%name,"invalid smart module");
            Err(ErrorCode::SmartStreamError(
                SmartStreamError::InvalidSmartStream(name),
            ))
        }
    } else {
        Err(ErrorCode::SmartStreamError(
            SmartStreamError::UndefinedSmartStream(name),
        ))
    }
}

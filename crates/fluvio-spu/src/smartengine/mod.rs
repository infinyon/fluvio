use anyhow::Error;
use tracing::{debug, error};

use dataplane::{
    ErrorCode, SmartStreamError, batch::Batch, record::Record, smartstream::SmartStreamRuntimeError,
};
use fluvio::{
    consumer::{
        Record as ConsumerRecord, SmartModuleInvocation, SmartStreamInvocation, SmartStreamKind,
    },
};
use fluvio_smartengine::{SmartStream, file_batch::FileBatchIterator};
use fluvio_spu_schema::server::stream_fetch::{
    SmartModuleInvocationWasm, SmartStreamPayload, SmartStreamWasm,
};
use futures_util::{StreamExt, stream::BoxStream};

use crate::core::DefaultSharedGlobalContext;

pub struct JoinStreamValue {
    stream: BoxStream<'static, Result<ConsumerRecord, ErrorCode>>,
    last_value: Option<ConsumerRecord>,
}

impl JoinStreamValue {
    async fn update(&mut self) -> Result<(), ErrorCode> {
        match self.stream.next().await {
            Some(Ok(record)) => {
                debug!(
                    offset = record.offset,
                    value_len = record.record.value().len(),
                    "received initial record from right join"
                );
                self.last_value = Some(record);
                Ok(())
            }
            Some(Err(e)) => Err(e),
            None => {
                debug!("right terminated");
                Err(ErrorCode::SmartStreamError(
                    SmartStreamError::JoinStreamTerminated("terminated".to_string()),
                ))
            }
        }
    }

    fn last_value(&self) -> Option<&Record> {
        self.last_value.as_ref().map(|r| r.inner())
    }
}

pub struct SmartStreamContext {
    pub smartstream: Box<dyn SmartStream>,
    pub right_consumer: Option<JoinStreamValue>,
}

impl SmartStreamContext {
    /// find wasm payload, they can be loaded from payload or from smart module
    /// smart module has precedent over payload
    pub async fn extract(
        wasm_payload: Option<SmartStreamPayload>,
        smart_module: Option<SmartModuleInvocation>,
        _smart_stream: Option<SmartStreamInvocation>,
        ctx: &DefaultSharedGlobalContext,
    ) -> Result<Option<Self>, ErrorCode> {
        match smart_module {
            Some(smart_module_invocation) => Ok(Some(
                Self::extract_smartmodule_context(smart_module_invocation, ctx).await?,
            )),
            None => {
                if let Some(payload) = wasm_payload {
                    Ok(Some(Self {
                        smartstream: Self::payload_to_smartstream(payload, ctx)?,
                        right_consumer: None,
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
        let right_consumer = match invocation.kind {
            // for join, create consumer stream
            SmartStreamKind::Join(ref topic) => {
                let consumer = ctx.leaders().partition_consumer(topic.to_owned(), 0).await;

                Some(JoinStreamValue {
                    stream: consumer
                        .stream(fluvio::Offset::beginning())
                        .await
                        .map_err(|err| {
                            error!("error fetching join data {}", err);
                            ErrorCode::SmartStreamJoinFetchError
                        })?
                        .boxed(),
                    last_value: None,
                })
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
            right_consumer,
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

    pub fn process_batch(
        &mut self,
        iter: &mut FileBatchIterator,
        max_bytes: usize,
    ) -> Result<(Batch, Option<SmartStreamRuntimeError>), Error> {
        self.smartstream.process_batch(
            iter,
            max_bytes,
            if let Some(consumer) = &self.right_consumer {
                consumer.last_value()
            } else {
                None
            },
        )
    }

    pub async fn update(&mut self) -> Result<(), ErrorCode> {
        if let Some(consumer) = self.right_consumer.as_mut() {
            consumer.update().await
        } else {
            Ok(())
        }
    }
}

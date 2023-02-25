use fluvio_smartengine::SmartModuleChainInstance;
use fluvio_protocol::link::ErrorCode;
use fluvio_spu_schema::server::{
    smartmodule::{SmartModuleInvocation, SmartModuleInvocationWasm},
};

use crate::core::DefaultSharedGlobalContext;
use crate::smartengine::chain;

pub struct SmartModuleContext {
    pub chain: SmartModuleChainInstance,
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

        let mut fetched_invocations = Vec::with_capacity(invocations.len());
        for invocation in invocations {
            fetched_invocations.push(resolve_invocation(invocation, ctx)?)
        }

        Ok(Some(Self {
            chain: chain::build_chain(fetched_invocations, version, ctx.smartengine_owned())?,
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

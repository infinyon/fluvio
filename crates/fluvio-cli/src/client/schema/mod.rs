// schema/mod.rs

pub use cmd::SchemaCmd;

mod apply;
mod create;
mod list;

mod cmd {
    use std::sync::Arc;
    use std::fmt::Debug;

    use anyhow::Result;
    use async_trait::async_trait;
    use clap::Parser;

    use fluvio::Fluvio;
    use fluvio_extension_common::target::ClusterTarget;

    use crate::client::cmd::ClientCmd;
    use crate::common::output::Terminal;

    use super::apply::ApplySchemaOpt;
    use super::create::CreateSchemaOpt;
    use super::list::ListSchemaOpt;

    #[derive(Debug, Parser)]
    pub enum SchemaCmd {
        Apply(ApplySchemaOpt),
        Create(CreateSchemaOpt),
        List(ListSchemaOpt),
    }

    #[async_trait]
    impl ClientCmd for SchemaCmd {
        async fn process<O: Terminal + Send + Sync + Debug>(
            self,
            out: Arc<O>,
            _target: ClusterTarget,
        ) -> Result<()> {
            match self {
                Self::Apply(opt) => {
                    opt.process(out).await?;
                }
                Self::Create(opt) => {
                    opt.process(out).await?;
                }
                Self::List(subcmd) => {
                    subcmd.process(out).await?;
                }
            }
            Ok(())
        }

        async fn process_client<O: Terminal + Debug + Send + Sync>(
            self,
            _out: Arc<O>,
            _fluvio: &Fluvio,
        ) -> Result<()> {
            Ok(())
        }
    }
}

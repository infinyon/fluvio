mod cli;
mod logs_output;
mod fetch_log_loop;
mod consume_hdlr;

use consume_hdlr::ConsumeOutputType;
pub use cli::ConsumeLogOpt;
pub use cli::ConsumeLogConfig;
use fetch_log_loop::fetch_log_loop;

use logs_output::process_fetch_topic_response;

pub use process::process_consume_log;

mod process {

    use log::debug;

    use crate::CliError;
    use crate::Terminal;

    use super::ConsumeLogOpt;
    use super::fetch_log_loop;

    /// Process Consume log cli request
    pub async fn process_consume_log<O>(
        out: std::sync::Arc<O>,
        opt: ConsumeLogOpt,
    ) -> Result<String, CliError>
    where
        O: Terminal,
    {
        let (target_server, cfg) = opt.validate()?;

        debug!("spu  leader consume config: {:#?}", cfg);

        let mut target = target_server.connect().await?;
        let consumer = target.consumer(&cfg.topic,cfg.partition).await?;
        fetch_log_loop(out, consumer, cfg).await?;
       
        Ok("ok".to_owned())
    }
}

use std::sync::Arc;
use serde::Serialize;
use structopt::StructOpt;
use prettytable::{Row, row, cell};

use crate::Result;
use fluvio::config::{ConfigFile, Config, TlsPolicy};
use fluvio_extension_common::Terminal;
use fluvio_extension_common::output::{TableOutputHandler, OutputType};

#[derive(Debug, StructOpt)]
pub struct ViewOpt {
    #[structopt(default_value, short, long)]
    output: OutputType,
}

impl ViewOpt {
    pub async fn process<O: Terminal>(self, out: Arc<O>) -> Result<()> {
        let config_file = ConfigFile::load(None)?;
        format_config_file(out, config_file.config(), self.output)?;
        Ok(())
    }
}

#[derive(Serialize)]
#[serde(transparent)]
struct ListConfig<'a>(&'a Config);

fn format_config_file<O: Terminal>(out: Arc<O>, config: &Config, mode: OutputType) -> Result<()> {
    let list_config = ListConfig(config);
    out.render_list(&list_config, mode)?;
    Ok(())
}

fn format_tls(tls: &TlsPolicy) -> &'static str {
    match tls {
        TlsPolicy::Verified(_) => "Verified",
        TlsPolicy::Anonymous => "Anonymous",
        TlsPolicy::Disabled => "Disabled",
    }
}

impl TableOutputHandler for ListConfig<'_> {
    fn header(&self) -> Row {
        row!["", "PROFILE", "CLUSTER", "ADDRESS", "TLS"]
    }

    fn content(&self) -> Vec<Row> {
        self.0
            .profile
            .iter()
            .map(|(profile_name, profile)| {
                let active = self
                    .0
                    .current_profile_name()
                    .map(|it| it == profile_name)
                    .map(|active| if active { "*" } else { "" })
                    .unwrap_or("");

                let (cluster, addr, tls) = self
                    .0
                    .cluster(&profile.cluster)
                    .map(|it| (&*profile.cluster, &*it.addr, format_tls(&it.tls)))
                    .unwrap_or(("", "", ""));

                row![active, profile_name, cluster, addr, tls,]
            })
            .collect()
    }

    fn errors(&self) -> Vec<String> {
        vec![]
    }
}

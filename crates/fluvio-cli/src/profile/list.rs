use std::sync::Arc;

use serde::Serialize;
use comfy_table::Row;
use clap::Parser;
use anyhow::Result;

use fluvio::config::{ConfigFile, Config, TlsPolicy};
use fluvio_extension_common::{Terminal, OutputFormat};
use fluvio_extension_common::output::{TableOutputHandler, OutputType};

#[derive(Debug, Parser)]
pub struct ListOpt {
    #[clap(flatten)]
    output: OutputFormat,
}

impl ListOpt {
    pub async fn process<O: Terminal>(self, out: Arc<O>) -> Result<()> {
        let config_file = match ConfigFile::load(None) {
            Ok(config) => config,
            Err(_) => {
                eprintln!("Unable to find Fluvio config");
                eprintln!("Try using 'fluvio cloud login' or 'fluvio cluster start'");
                return Ok(());
            }
        };
        format_config_file(out, config_file.config(), self.output.format)?;
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
        Row::from(["", "PROFILE", "CLUSTER", "ADDRESS", "TLS"])
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
                    .map(|it| (&*profile.cluster, &*it.endpoint, format_tls(&it.tls)))
                    .unwrap_or(("", "", ""));

                Row::from([active, profile_name, cluster, addr, tls])
            })
            .collect()
    }

    fn errors(&self) -> Vec<String> {
        vec![]
    }
}

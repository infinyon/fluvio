use std::sync::Arc;

use fluvio_extension_common::installation::InstallationType;
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
        Row::from(["", "PROFILE", "CLUSTER", "ADDRESS", "TLS", "INSTALLATION"])
    }

    fn content(&self) -> Vec<Row> {
        let mut profile_names = self.0.profile.keys().collect::<Vec<_>>();
        profile_names.sort();
        profile_names
            .iter()
            .filter_map(|profile_name| {
                let profile = self.0.profile.get(profile_name.as_str())?;
                let active = self
                    .0
                    .current_profile_name()
                    .map(|it| it == profile_name.as_str())
                    .map(|active| if active { "*" } else { "" })
                    .unwrap_or("");

                let (cluster, addr, tls, installation) = self
                    .0
                    .cluster(&profile.cluster)
                    .map(|it| {
                        (
                            &*profile.cluster,
                            &*it.endpoint,
                            format_tls(&it.tls),
                            InstallationType::load(it).to_string(),
                        )
                    })
                    .unwrap_or(("", "", "", String::new()));
                let row = Row::from([active, profile_name, cluster, addr, tls, &installation]);
                Some(row)
            })
            .collect()
    }

    fn errors(&self) -> Vec<String> {
        vec![]
    }
}

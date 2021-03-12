use std::process::Command;
use structopt::StructOpt;
use fluvio_extension_common::FluvioExtensionMetadata;
use fluvio_extension_consumer::topic::TopicCmd;
use fluvio_extension_consumer::partition::PartitionCmd;
use fluvio_extension_consumer::produce::ProduceLogOpt;
use fluvio_extension_consumer::consume::ConsumeLogOpt;
use fluvio_command::CommandExt;
use crate::Result;

#[derive(Debug, StructOpt)]
pub struct MetadataOpt {}
impl MetadataOpt {
    pub fn process(self) -> Result<()> {
        let metadata = Self::metadata()?;
        if let Ok(out) = serde_json::to_string(&metadata) {
            println!("{}", out);
        }

        Ok(())
    }

    fn metadata() -> Result<Vec<FluvioExtensionMetadata>> {
        let mut metadata = vec![
            TopicCmd::metadata(),
            PartitionCmd::metadata(),
            ProduceLogOpt::metadata(),
            ConsumeLogOpt::metadata(),
        ];

        if let Ok(subcommand_meta) = subcommand_metadata() {
            let extension_meta = subcommand_meta.into_iter().map(|it| it.meta);
            metadata.extend(extension_meta);
        }

        Ok(metadata)
    }
}

pub struct SubcommandMetadata {
    pub filename: String,
    pub meta: FluvioExtensionMetadata,
}

/// Collects the metadata of Fluvio extensions installed on the system
pub fn subcommand_metadata() -> Result<Vec<SubcommandMetadata>> {
    let mut metadata = Vec::new();

    for (filename, path) in crate::install::get_extensions()? {
        let result = Command::new(&path).arg("metadata").result();
        let output = match result {
            Ok(out) => out.stdout,
            _ => continue,
        };

        let json_result = serde_json::from_slice::<FluvioExtensionMetadata>(&output);
        if let Ok(meta) = json_result {
            let subcommand = SubcommandMetadata { filename, meta };
            metadata.push(subcommand);
        }
    }

    Ok(metadata)
}

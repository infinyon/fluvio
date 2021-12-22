use std::sync::Arc;

use structopt::StructOpt;

use fluvio::metadata::derivedstream::DerivedStreamSpec;
use fluvio::Fluvio;

use crate::common::output::Terminal;
use crate::common::OutputFormat;
use crate::Result;

/// List all existing SmartModules
#[derive(Debug, StructOpt)]
pub struct ListDerivedStreamOpt {
    #[structopt(flatten)]
    output: OutputFormat,
}

impl ListDerivedStreamOpt {
    pub async fn process<O: Terminal>(self, out: Arc<O>, fluvio: &Fluvio) -> Result<()> {
        let admin = fluvio.admin().await;
        let lists = admin.list::<DerivedStreamSpec, _>(vec![]).await?;
        output::smart_stream_response_to_output(out, lists, self.output.format)
    }
}
mod output {

    //!
    //! # Fluvio SC - output processing
    //!
    //! Format Smart Stream response based on output type

    use prettytable::Row;
    use prettytable::row;
    use prettytable::Cell;
    use prettytable::cell;
    use prettytable::format::Alignment;
    use tracing::debug;
    use serde::Serialize;
    use fluvio_extension_common::output::OutputType;
    use fluvio_extension_common::Terminal;

    use fluvio::metadata::objects::Metadata;
    use fluvio::metadata::derivedstream::DerivedStreamSpec;

    use crate::CliError;
    use fluvio_extension_common::output::TableOutputHandler;
    use fluvio_extension_common::t_println;

    #[derive(Serialize)]
    struct ListDerivedStream(Vec<Metadata<DerivedStreamSpec>>);

    // -----------------------------------
    // Format Output
    // -----------------------------------

    pub fn smart_stream_response_to_output<O: Terminal>(
        out: std::sync::Arc<O>,
        list_smart_streams: Vec<Metadata<DerivedStreamSpec>>,
        output_type: OutputType,
    ) -> Result<(), CliError> {
        debug!("derived streams: {:#?}", list_smart_streams);

        if !list_smart_streams.is_empty() {
            let smart_streams = ListDerivedStream(list_smart_streams);
            out.render_list(&smart_streams, output_type)?;
            Ok(())
        } else {
            t_println!(out, "no derived-streams");
            Ok(())
        }
    }

    // -----------------------------------
    // Output Handlers
    // -----------------------------------
    impl TableOutputHandler for ListDerivedStream {
        /// table header implementation
        fn header(&self) -> Row {
            row!["NAME", "STATUS", "INPUT", "STEPS"]
        }

        /// return errors in string format
        fn errors(&self) -> Vec<String> {
            vec![]
        }

        /// table content implementation
        fn content(&self) -> Vec<Row> {
            self.0
                .iter()
                .map(|r| {
                    let spec = &r.spec;
                    Row::new(vec![
                        Cell::new_align(&r.name, Alignment::RIGHT),
                        Cell::new_align(&r.status.to_string(), Alignment::RIGHT),
                        Cell::new_align(&spec.input.to_string(), Alignment::RIGHT),
                        Cell::new_align(&spec.steps.to_string(), Alignment::RIGHT),
                    ])
                })
                .collect()
        }
    }
}

mod cli;

pub use cli::ProduceLogOpt;
use cli::FileRecord;
use cli::ProduceLogConfig;
pub use produce::process_produce_record;

pub type RecordTuples = Vec<(String, Vec<u8>)>;

mod produce {

    use log::debug;
    use futures::stream::StreamExt;

    use flv_future_aio::fs::File;
    use flv_future_aio::io::stdin;
    use flv_future_aio::io::ReadExt;
    use flv_future_aio::io::BufReader;
    use flv_future_aio::io::AsyncBufReadExt;
    use flv_types::{print_cli_err, print_cli_ok};
    use flv_client::ReplicaLeader;

    use flv_client::profile::ReplicaLeaderTargetInstance;
    use crate::CliError;
    use crate::Terminal;
    use crate::t_println;

    use super::RecordTuples;
    use super::ProduceLogOpt;
    use super::FileRecord;
    use super::ProduceLogConfig;

    // -----------------------------------
    //  Fluvio SPU - Process Request
    // -----------------------------------

    // -----------------------------------
    //  CLI Processing
    // -----------------------------------

    /// Process produce record cli request
    pub async fn process_produce_record<O>(
        out: std::sync::Arc<O>,
        opt: ProduceLogOpt,
    ) -> Result<String, CliError>
    where
        O: Terminal,
    {
        let (target_server, cfg) = opt.validate()?;
        let file_records = file_to_records(&cfg.records_form_file).await?;

        (match target_server.connect(&cfg.topic, cfg.partition).await? {
            ReplicaLeaderTargetInstance::Kf(leader) => {
                render_produce_record(out, leader, cfg, file_records).await
            }
            ReplicaLeaderTargetInstance::Spu(leader) => {
                render_produce_record(out, leader, cfg, file_records).await
            }
        })
        .map(|_| format!(""))
    }

    /// Dispatch records based on the content of the record tuples variable
    async fn render_produce_record<O: Terminal, L: ReplicaLeader>(
        out: std::sync::Arc<O>,
        mut leader: L,
        opt: ProduceLogConfig,
        record_tuples: RecordTuples,
    ) -> Result<(), CliError> {
        // in both cases, exit loop on error
        if record_tuples.len() > 0 {
            // records from files
            for r_tuple in record_tuples {
                t_println!(out, "{}", r_tuple.0);
                process_record(&mut leader, r_tuple.1).await;
            }
        } else {
            let stdin = stdin();
            let mut lines = BufReader::new(stdin).lines();
            while let Some(line) = lines.next().await {
                let text = line?;
                debug!("read lines {} bytes", text);
                let record = text.as_bytes().to_vec();
                process_record(&mut leader, record).await;
                if !opt.continuous {
                    return Ok(());
                }
            }
        }

        debug!("done sending records");

        Ok(())
    }

    /// Process record and print success or error
    /// TODO: Add version handling for SPU
    async fn process_record<L: ReplicaLeader>(spu: &mut L, record: Vec<u8>) {
        match spu.send_record(record).await {
            Ok(()) => {
                debug!("record send success");
                print_cli_ok!()
            }
            Err(err) => {
                print_cli_err!(format!("error processing record: {}", err));
                std::process::exit(-1);
            }
        }
    }

    /// Retrieve one or more files and converts them into a list of (name, record) touples
    async fn file_to_records(
        file_record_options: &Option<FileRecord>,
    ) -> Result<RecordTuples, CliError> {
        let mut records: RecordTuples = vec![];

        match file_record_options {
            Some(file_record) => {
                match file_record {
                    // lines as records
                    FileRecord::Lines(lines2rec_path) => {
                        let f = File::open(lines2rec_path).await?;
                        let mut lines = BufReader::new(f).lines();
                        // reach each line and convert to byte array
                        for line in lines.next().await {
                            if let Ok(text) = line {
                                records.push((text.clone(), text.as_bytes().to_vec()));
                            }
                        }
                    }

                    // files as records
                    FileRecord::Files(files_to_rec_path) => {
                        for file_path in files_to_rec_path {
                            let file_name = file_path.to_str().unwrap_or("?");
                            let mut f = File::open(file_path).await?;
                            let mut buffer = Vec::new();

                            // read the whole file in a byte array
                            f.read_to_end(&mut buffer).await?;
                            records.push((file_name.to_owned(), buffer));
                        }
                    }
                }
            }
            None => {}
        }

        Ok(records)
    }
}

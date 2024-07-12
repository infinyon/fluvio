use std::path::PathBuf;
use std::sync::Arc;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use comfy_table::{Table, Row, Cell, CellAlignment};
use fluvio::stats::{
    ClientStatsDataCollect, ClientStatsDataFrame, ClientStatsMetric, ClientStatsMetricRaw,
};
use indicatif::ProgressBar;
use fluvio::TopicProducer;
use fluvio::spu::SpuPool;
use crate::Result;
use crate::error::CliError;

/// Creates (or truncates) a new file, and writes a CSV header at top for stats report purposes
/// Header contents dependent on what stats producer configured to collect
/// Column names include units
pub async fn start_csv_report(
    stats_path: &PathBuf,
    producer: &Arc<TopicProducerPool>,
) -> Result<BufWriter<File>, crate::error::CliError> {
    let mut stats_file = BufWriter::new(
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(stats_path)?,
    );
    if let Some(stats) = producer.stats() {
        write_line_to_file(
            Some(&mut stats_file),
            stats
                .csv_header(&stats.stats_collect().to_metrics())?
                .as_bytes(),
        )?;
        stats_file.flush()?;
    }
    Ok(stats_file)
}

/// Writes a row of CSV data to stats report file
/// Header contents dependent on what stats producer configured to collect
pub async fn write_csv_dataframe(
    producer: &Arc<TopicProducerPool>,
    last_update_check: i64,
    maybe_stats_file: Option<&mut BufWriter<File>>,
) -> Result<i64, CliError> {
    let last_update = if let Some(dataframe_sample) = &producer.stats() {
        let last_update = if let ClientStatsMetricRaw::LastUpdated(last) =
            dataframe_sample.get(ClientStatsMetric::LastUpdated)
        {
            last
        } else {
            0
        };

        if last_update != last_update_check {
            let dataframe =
                dataframe_sample.csv_dataframe(&dataframe_sample.stats_collect().to_metrics())?;

            write_line_to_file(maybe_stats_file, dataframe.as_bytes())?;
            last_update
        } else {
            last_update
        }
    } else {
        last_update_check
    };

    Ok(last_update)
}

/// Helper function for writing CSV data to file
pub fn write_line_to_file(
    maybe_file: Option<&mut BufWriter<File>>,
    line: &[u8],
) -> Result<(), CliError> {
    if let Some(stats_file) = maybe_file {
        let _ = stats_file.write(line)?;
    }
    Ok(())
}

/// Format current stats into `comfy_table` for alignment purposes
/// Table contents dependent on what stats producer configured to collect
/// Resulting `String` is printed by `indicatif::ProgressBar`
pub fn format_current_stats(client_stats: ClientStatsDataFrame) -> String {
    let mut t = Table::new();
    t.load_preset(comfy_table::presets::NOTHING);

    let mut r = Row::new();

    if client_stats.stats_collect() == ClientStatsDataCollect::All
        || client_stats.stats_collect() == ClientStatsDataCollect::Data
    {
        r.add_cell(Cell::new("throughput/s").set_alignment(CellAlignment::Center))
            .add_cell(
                Cell::new(
                    client_stats
                        .get_format(ClientStatsMetric::SecondThroughput)
                        .value_to_string(),
                )
                .set_alignment(CellAlignment::Right),
            );
        r.add_cell(Cell::new("avg latency/s").set_alignment(CellAlignment::Center))
            .add_cell(
                Cell::new(
                    client_stats
                        .get_format(ClientStatsMetric::SecondMeanLatency)
                        .value_to_string(),
                )
                .set_alignment(CellAlignment::Right),
            );
    }

    if client_stats.stats_collect() == ClientStatsDataCollect::All
        || client_stats.stats_collect() == ClientStatsDataCollect::System
    {
        r.add_cell(Cell::new("memory").set_alignment(CellAlignment::Center))
            .add_cell(
                Cell::new(
                    client_stats
                        .get_format(ClientStatsMetric::Mem)
                        .value_to_string(),
                )
                .set_alignment(CellAlignment::Right),
            );
        r.add_cell(Cell::new("CPU %").set_alignment(CellAlignment::Center))
            .add_cell(
                Cell::new(
                    client_stats
                        .get_format(ClientStatsMetric::Cpu)
                        .value_to_string(),
                )
                .set_alignment(CellAlignment::Right),
            );
    }

    t.add_row(r);

    format!("{t}")
}

/// Format summary stats into `comfy_table` for alignment purposes
/// Table contents dependent on what stats producer configured to collect
/// Resulting `String` is printed by `indicatif::ProgressBar`
pub async fn format_summary_stats(client_stats: ClientStatsDataFrame) -> String {
    let mut t = Table::new();
    t.load_preset(comfy_table::presets::NOTHING);

    let mut r = Row::new();

    if client_stats.stats_collect() == ClientStatsDataCollect::All
        || client_stats.stats_collect() == ClientStatsDataCollect::Data
    {
        r.add_cell(Cell::new("total throughput").set_alignment(CellAlignment::Center))
            .add_cell(
                Cell::new(
                    client_stats
                        .get_format(ClientStatsMetric::Throughput)
                        .value_to_string(),
                )
                .set_alignment(CellAlignment::Right),
            );
        r.add_cell(Cell::new("total data").set_alignment(CellAlignment::Center))
            .add_cell(
                Cell::new(
                    client_stats
                        .get_format(ClientStatsMetric::Bytes)
                        .value_to_string(),
                )
                .set_alignment(CellAlignment::Right),
            );
        r.add_cell(Cell::new("records transferred").set_alignment(CellAlignment::Center))
            .add_cell(
                Cell::new(
                    client_stats
                        .get_format(ClientStatsMetric::Records)
                        .value_to_string(),
                )
                .set_alignment(CellAlignment::Right),
            );
    }
    r.add_cell(Cell::new("run time").set_alignment(CellAlignment::Center))
        .add_cell(
            Cell::new(
                client_stats
                    .get_format(ClientStatsMetric::RunTime)
                    .value_to_string(),
            )
            .set_alignment(CellAlignment::Right),
        );

    t.add_row(r);

    format!("{t}")
}

/// Report the producer summary to stdout with the `ProgressBar`
pub async fn producer_summary(
    producer: &Arc<TopicProducerPool>,
    maybe_stats_bar: Option<&ProgressBar>,
    force_print_stats: bool,
) {
    if let Some(producer_stats) = producer.stats() {
        if let Some(stats_bar) = maybe_stats_bar {
            stats_bar.set_message(format_summary_stats(producer_stats).await);
        } else if force_print_stats {
            println!("{}", format_summary_stats(producer_stats).await);
            println!();
        }
    } else if force_print_stats {
        println!("Stats were not collected for summary")
    }
}

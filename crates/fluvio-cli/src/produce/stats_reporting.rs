use std::path::PathBuf;
use std::sync::Arc;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use comfy_table::{Table, Row, Cell, CellAlignment};
use fluvio::stats::{ClientStatsDataCollect, ClientStatsDataPoint};
use indicatif::ProgressBar;
use fluvio::TopicProducer;
use crate::Result;
use crate::error::CliError;

/// Creates (or truncates) a new file, and writes a CSV header at top for stats report purposes
/// Header contents dependent on what stats producer configured to collect
/// Column names include units
pub(crate) async fn start_csv_report(
    stats_path: &PathBuf,
    producer: &Arc<TopicProducer>,
) -> Result<BufWriter<File>, crate::error::CliError> {
    let mut stats_file = BufWriter::new(
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(stats_path)?,
    );
    if let Some(stats) = producer.stats().await {
        write_line_to_file(Some(&mut stats_file), stats.csv_header()?.as_bytes())?;
        stats_file.flush()?;
    }
    Ok(stats_file)
}

/// Writes a row of CSV data to stats report file
/// Header contents dependent on what stats producer configured to collect
pub(crate) async fn write_csv_datapoint(
    producer: &Arc<TopicProducer>,
    datapoint_check: &str,
    maybe_stats_file: Option<&mut BufWriter<File>>,
) -> Result<String, CliError> {
    let datapoint = if let Some(datapoint_sample) = &producer.stats().await {
        let datapoint = datapoint_sample.csv_datapoint()?;

        if datapoint != *datapoint_check {
            write_line_to_file(maybe_stats_file, datapoint.as_bytes())?;
            datapoint
        } else {
            datapoint_check.to_string()
        }
    } else {
        datapoint_check.to_string()
    };

    Ok(datapoint)
}

/// Helper function for writing CSV data to file
fn write_line_to_file(
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
pub(crate) async fn format_current_stats(client_stats: ClientStatsDataPoint) -> String {
    //println!("Datapoint: {:#?}", client_stats);

    let mut t = Table::new();
    t.load_preset(comfy_table::presets::NOTHING);

    let mut r = Row::new();

    if client_stats.stats_collect() == ClientStatsDataCollect::All
        || client_stats.stats_collect() == ClientStatsDataCollect::Data
    {
        r.add_cell(Cell::new("throughput").set_alignment(CellAlignment::Center))
            .add_cell(
                Cell::new(format!("{:<15.3}", client_stats.throughput()))
                    .set_alignment(CellAlignment::Right),
            );
        r.add_cell(Cell::new("latency").set_alignment(CellAlignment::Center))
            .add_cell(
                Cell::new(format!("{:<15.3}", client_stats.last_latency()))
                    .set_alignment(CellAlignment::Right),
            );
    }

    if client_stats.stats_collect() == ClientStatsDataCollect::All
        || client_stats.stats_collect() == ClientStatsDataCollect::System
    {
        r.add_cell(Cell::new("memory").set_alignment(CellAlignment::Center))
            .add_cell(
                Cell::new(format!("{:<15.3}", client_stats.memory()))
                    .set_alignment(CellAlignment::Right),
            );
        r.add_cell(Cell::new("CPU").set_alignment(CellAlignment::Center))
            .add_cell(
                Cell::new(format!("{:<15.3}", client_stats.cpu()))
                    .set_alignment(CellAlignment::Right),
            );
    }

    t.add_row(r);

    format!("{t}")
}

/// Format summary stats into `comfy_table` for alignment purposes
/// Table contents dependent on what stats producer configured to collect
/// Resulting `String` is printed by `indicatif::ProgressBar`
pub(crate) async fn format_summary_stats(client_stats: ClientStatsDataPoint) -> String {
    let mut t = Table::new();
    t.load_preset(comfy_table::presets::NOTHING);

    let mut r = Row::new();

    if client_stats.stats_collect() == ClientStatsDataCollect::All
        || client_stats.stats_collect() == ClientStatsDataCollect::Data
    {
        r.add_cell(Cell::new("total throughput").set_alignment(CellAlignment::Center))
            .add_cell(
                Cell::new(format!("{:<15.3}", client_stats.total_throughput()))
                    .set_alignment(CellAlignment::Right),
            );
        r.add_cell(Cell::new("total data").set_alignment(CellAlignment::Center))
            .add_cell(
                Cell::new(format!("{:<10.3}", client_stats.total_bytes()))
                    .set_alignment(CellAlignment::Right),
            );
        r.add_cell(Cell::new("records transferred").set_alignment(CellAlignment::Center))
            .add_cell(
                Cell::new(format!("{:<10.3}", client_stats.records()))
                    .set_alignment(CellAlignment::Right),
            );
    }

    r.add_cell(Cell::new("uptime").set_alignment(CellAlignment::Center))
        .add_cell(
            Cell::new(format!("{:<10.3}", client_stats.uptime()))
                .set_alignment(CellAlignment::Right),
        );

    t.add_row(r);

    format!("{t}")
}

/// Report the producer summary to stdout with the `ProgressBar`
pub(crate) async fn producer_summary(producer: &Arc<TopicProducer>, stats_bar: &ProgressBar) {
    if let Some(producer_stats) = producer.stats().await {
        stats_bar.set_message(format_summary_stats(producer_stats).await);
        stats_bar.println(" ");
    }
}

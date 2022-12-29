use std::{path::PathBuf};

use clap::Parser;
use anyhow::{Result, anyhow};

use fluvio_protocol::record::Offset;
use fluvio_future::task::run_block_on;
use fluvio_storage::{
    LogIndex, OffsetPosition,
    batch_header::BatchHeaderStream,
    segment::{MutableSegment},
    config::{ReplicaConfig},
};
use fluvio_storage::records::FileRecords;

///
/// Bunch of storage utilities:
///
/// validation: `cargo run --bin storage-cli --features=cli --release validate ~/.fluvio/data/spu-logs-5001/longevity-0 --skip-errors=false `
#[derive(Debug, Parser)]
#[clap(name = "storage", about = "Flavio Storage CLI")]
enum Main {
    /// validate log
    #[clap(name = "log")]
    Log(LogOpt),

    #[clap(name = "index")]
    Index(IndexOpt),

    #[clap(name = "validate")]
    ValidateSegment(SegmentValidateOpt),
}

fn main() {
    fluvio_future::subscriber::init_logger();

    let opt = Main::parse();

    let result = run_block_on(async {
        match opt {
            Main::Log(opt) => dump_log(opt).await,
            Main::Index(opt) => dump_index(opt).await,
            Main::ValidateSegment(opt) => validate_segment(opt).await,
        }
    });
    if let Err(err) = result {
        println!("error in async: {:#?}", err)
    };
}

#[derive(Debug, Parser)]
pub(crate) struct LogOpt {
    #[clap(value_parser)]
    file_name: PathBuf,

    #[clap(long)]
    max: Option<usize>,

    #[clap(long)]
    min: Option<usize>,

    /// write offsets
    #[clap(long)]
    print: bool,

    /// set position
    #[clap(long, default_value = "0")]
    position: u32,
}

async fn dump_log(opt: LogOpt) -> Result<()> {
    if let Some(max) = opt.max {
        if let Some(min) = opt.min {
            if min > max {
                return Err(anyhow!("min > max"));
            }
        }
    }

    println!("opening: {:#?} position: {}", opt.file_name, opt.position);

    let mut header_stream = BatchHeaderStream::open(opt.file_name).await?;
    header_stream.set_absolute(opt.position).await?;

    //  println!("base offset: {}",batch_stream.get_base_offset());

    let mut count: usize = 0;
    let time = std::time::Instant::now();
    while let Some(batch_pos) = header_stream.try_next().await? {
        let pos = batch_pos.get_pos();
        let batch = batch_pos.inner();

        let base_offset = batch.get_base_offset();

        if let Some(min) = opt.min {
            if (base_offset as usize) < min {
                continue;
            }
        }
        if let Some(max) = opt.max {
            if (base_offset as usize) > max {
                break;
            }
        }

        if opt.print {
            println!(
                "batch offset: {}, pos: {}, len: {}, ",
                base_offset, pos, batch.batch_len,
            );
        }

        count += 1;
    }

    println!(
        "{} records checked in {} millsecs",
        count,
        time.elapsed().as_millis()
    );

    Ok(())
}

#[derive(Debug, Parser)]
pub(crate) struct IndexOpt {
    #[clap(value_parser)]
    file_name: PathBuf,

    #[clap(long, default_value = "100")]
    max: usize,

    #[clap(long, default_value = "0")]
    min: usize,
}

async fn dump_index(opt: IndexOpt) -> Result<()> {
    if opt.min > opt.max {
        return Err(anyhow!("min > max"));
    }

    let log = LogIndex::open_from_path(opt.file_name).await?;

    println!("has {} bytes", log.len());
    let max_entries = std::cmp::min(opt.max, log.len());
    let mut count: usize = 0;
    let mut display: usize = 0;
    for i in opt.min..max_entries {
        let (offset, pos) = log[i].to_be();
        if offset > 0 && pos > 0 {
            count += 1;
            if count < opt.max {
                println!("i: {} offset: {}  pos: {}", i, offset, pos);
                display += 1;
            }
        }
    }
    if count > opt.max {
        println!("there was {} entries only {} was displayed", count, display);
    } else {
        println!("there was {} entries:", count);
    }

    Ok(())
}

#[derive(Debug, Parser)]
pub(crate) struct SegmentValidateOpt {
    #[clap(value_parser)]
    file_name: PathBuf,

    #[clap(long, default_value = "0")]
    base_offset: Offset,
}

pub(crate) async fn validate_segment(opt: SegmentValidateOpt) -> Result<()> {
    let file_path = opt.file_name;

    let option = ReplicaConfig::builder()
        .base_dir(file_path.clone())
        .build()
        .shared();
    let mut active_segment = MutableSegment::open_for_write(opt.base_offset, option)
        .await
        .expect("failed to open segment");

    println!(
        "performing validation on segment: {:#?}",
        active_segment.get_msg_log().get_path()
    );

    let start = std::time::Instant::now();
    let last_offset = active_segment.validate_and_repair().await?;

    let duration = start.elapsed().as_secs_f32();

    println!("completed, last offset = {last_offset}, took: {duration} seconds");

    Ok(())
}

use std::{path::PathBuf};

use dataplane::Offset;
use structopt::StructOpt;

use fluvio_future::task::run_block_on;
use fluvio_storage::{
    LogIndex, StorageError, OffsetPosition,
    batch_header::BatchHeaderStream,
    segment::{MutableSegment},
    config::{ReplicaConfig},
};

///
/// Bunch of storage utilities:
///
/// validation: `cargo run --release --bin storage-cli --features=cli validate  ~/.fluvio/data/spu-logs-5001/longevity-0 `
#[derive(Debug, StructOpt)]
#[structopt(name = "storage", about = "Flavio Storage CLI")]
enum Main {
    #[structopt(name = "log")]
    Log(LogOpt),

    #[structopt(name = "index")]
    Index(IndexOpt),

    #[structopt(name = "validate_segment")]
    ValidateSegment(SegmentValidateOpt),
}

fn main() {
    fluvio_future::subscriber::init_logger();

    let opt = Main::from_args();

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

#[derive(Debug, StructOpt)]
pub(crate) struct LogOpt {
    #[structopt(parse(from_os_str))]
    file_name: PathBuf,

    #[structopt(long, default_value = "100")]
    max: usize,

    #[structopt(long, default_value = "0")]
    min: usize,
}

async fn dump_log(opt: LogOpt) -> Result<(), StorageError> {
    if opt.min > opt.max {
        return Err(StorageError::Other("min > max".to_string()));
    }

    let mut header = BatchHeaderStream::open(opt.file_name).await?;

    //  println!("base offset: {}",batch_stream.get_base_offset());

    while let Some(batch_pos) = header.next().await {
        let base_offset = batch_pos.get_batch().get_base_offset();
        if base_offset as usize >= opt.min && base_offset as usize <= opt.max {
            println!(
                "batch offset: {}, pos: {}, len: {}, ",
                base_offset,
                batch_pos.get_pos(),
                batch_pos.len(),
            );
        }
    }

    if let Some(invalid) = header.invalid() {
        println!("invalid: {:#?}", invalid);
    } else {
        println!("all checked");
    }

    Ok(())
}

#[derive(Debug, StructOpt)]
pub(crate) struct IndexOpt {
    #[structopt(parse(from_os_str))]
    file_name: PathBuf,

    #[structopt(long, default_value = "100")]
    max: usize,

    #[structopt(long, default_value = "0")]
    min: usize,
}

async fn dump_index(opt: IndexOpt) -> Result<(), StorageError> {
    if opt.min > opt.max {
        return Err(StorageError::Other("min > max".to_string()));
    }

    let log = LogIndex::open_from_path(opt.file_name).await?;

    println!("has {} bytes", log.len());
    let max_entries = std::cmp::min(opt.max, log.len());
    let mut count: usize = 0;
    let mut display: usize = 0;
    for i in opt.min..max_entries {
        let (offset, pos) = log[i as usize].to_be();
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

#[derive(Debug, StructOpt)]
pub(crate) struct SegmentValidateOpt {
    #[structopt(parse(from_os_str))]
    file_name: PathBuf,

    #[structopt(long, default_value = "0")]
    base_offset: Offset,

    #[structopt(long)]
    skip_errors: bool,

    #[structopt(long)]
    verbose: bool,
}

pub(crate) async fn validate_segment(opt: SegmentValidateOpt) -> Result<(), StorageError> {
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
        file_path.display()
    );

    let start = std::time::Instant::now();
    let last_offset = active_segment
        .validate(opt.skip_errors, opt.verbose)
        .await?;

    let duration = start.elapsed().as_secs_f32();

    println!("completed, last offset = {last_offset}, took: {duration} seconds");

    Ok(())
}

#[derive(Debug, StructOpt)]
pub(crate) struct ReplicaOpt {
    #[structopt(parse(from_os_str))]
    dir_name: PathBuf,

    #[structopt(long)]
    skip_errors: bool,

    #[structopt(long)]
    verbose: bool,
}

pub(crate) async fn validate_replica(opt: ReplicaOpt) -> Result<(), StorageError> {
    let dir_name = opt.dir_name;

    let option = ReplicaConfig::builder()
        .base_dir(dir_name.clone())
        .build()
        .shared();

    /*
    let mut active_segment = MutableSegment::open_for_write(opt.base_offset, option)
        .await
        .expect("failed to open segment");

    println!(
        "performing validation on segment: {:#?}",
        file_path.display()
    );

    let start = std::time::Instant::now();
    let last_offset = active_segment
        .validate(opt.skip_errors, opt.verbose)
        .await?;

    let duration = start.elapsed().as_secs_f32();

    println!("completed, last offset = {last_offset}, took: {duration} seconds");
    */

    Ok(())
}

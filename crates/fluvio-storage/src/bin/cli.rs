use std::path::PathBuf;

use structopt::StructOpt;

use fluvio_future::task::run_block_on;
use fluvio_storage::{LogIndex, StorageError, OffsetPosition, batch_header::BatchHeaderStream};

#[derive(Debug, StructOpt)]
#[structopt(name = "storage", about = "Flavio Storage CLI")]
enum Main {
    #[structopt(name = "log")]
    Log(LogOpt),
    #[structopt(name = "index")]
    Index(IndexOpt),
}

fn main() {
    fluvio_future::subscriber::init_logger();

    let opt = Main::from_args();

    match opt {
        Main::Log(opt) => dump_log(opt),
        Main::Index(opt) => dump_index(opt),
    }
}

#[derive(Debug, StructOpt)]
pub(crate) struct LogOpt {
    #[structopt(parse(from_os_str))]
    file_name: PathBuf,
}

async fn print_logs(path: PathBuf) -> Result<(), StorageError> {
    let mut header = BatchHeaderStream::open(path).await?;

    //  println!("base offset: {}",batch_stream.get_base_offset());

    while let Some(batch_pos) = header.next().await {
        println!(
            "batch offset: {}, pos: {}, len: {}, ",
            batch_pos.get_batch().get_base_offset(),
            batch_pos.get_pos(),
            batch_pos.len(),
        );
    }

    if let Some(invalid) = header.invalid() {
        println!("invalid: {:#?}", invalid);
    } else {
        println!("all checked");
    }

    Ok(())
}

pub(crate) fn dump_log(opt: LogOpt) {
    let file_path = opt.file_name;

    println!("dumping batch: {:#?}", file_path);
    let ft = print_logs(file_path);
    let result = run_block_on(ft);
    if let Err(err) = result {
        println!("error in async: {:#?}", err)
    };
}

#[derive(Debug, StructOpt)]
pub(crate) struct IndexOpt {
    #[structopt(parse(from_os_str))]
    file_name: PathBuf,
}

pub(crate) fn dump_index(opt: IndexOpt) {
    let file_path = opt.file_name;

    println!("dumping index: {:#?}", file_path);
    let ft = print_index(file_path);
    let result = run_block_on(ft);
    if let Err(err) = result {
        println!("error in async: {:#?}", err)
    };
}

const MAX: u32 = 100000;

async fn print_index(path: PathBuf) -> Result<(), StorageError> {
    let log = LogIndex::open_from_path(path).await?;

    println!("has {} bytes", log.len());
    let entries = log.len();
    let mut count: u32 = 0;
    let mut display: u32 = 0;
    for i in 0..entries {
        let (offset, pos) = log[i as usize].to_be();
        if offset > 0 && pos > 0 {
            count += 1;
            if count < MAX {
                println!("i: {} offset: {}  pos: {}", i, offset, pos);
                display += 1;
            }
        }
    }
    if count > MAX {
        println!("there was {} entries only {} was displayed", count, display);
    } else {
        println!("there was {} entries:", count);
    }

    Ok(())
}

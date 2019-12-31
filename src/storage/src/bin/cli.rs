// Storage CLi
#![feature(async_await)]

use std::path::PathBuf;
use std::io::Error as IoError;

use structopt::StructOpt;
use futures::stream::StreamExt;

use flv_future_aio::fs::file_util;
use flv_future_core::run_block_on;

use storage::DefaultFileBatchStream;
use storage::LogIndex;
use storage::StorageError;
use storage::OffsetPosition;

#[derive(Debug, StructOpt)]
#[structopt(name = "storage", about = "Flavio Storage CLI")]
enum Main {
    #[structopt(name = "log")]
    Log(LogOpt),
    #[structopt(name = "index")]
    Index(IndexOpt),
}

fn main() {
    utils::init_logger();

    let opt = Main::from_args();

    let res = match opt {
        Main::Log(opt) => dump_log(opt),
        Main::Index(opt) => dump_index(opt),
    };

    if let Err(err) = res {
        println!("error occured: {:#?}", err)
    }
}

#[derive(Debug, StructOpt)]
pub(crate) struct LogOpt {
    #[structopt(parse(from_os_str))]
    file_name: PathBuf,
}

async fn print_logs(path: PathBuf) -> Result<(), IoError> {
    let file = file_util::open(path).await?;

    let mut batch_stream = DefaultFileBatchStream::new(file);

    //  println!("base offset: {}",batch_stream.get_base_offset());

    while let Some(file_batch) = batch_stream.next().await {
        // let batch_base_offset = batch.get_base_offset();
        let batch = file_batch.get_batch();
        //let header = batch.get_header();
        // let offset_delta = header.last_offset_delta;

        println!(
            "batch offset: {}, len: {}, pos: {}",
            batch.get_base_offset(),
            file_batch.len(),
            file_batch.get_pos()
        );

        for record in &batch.records {
            println!("record offset: {}", record.get_offset_delta());
        }
    }

    Ok(())
}

pub(crate) fn dump_log(opt: LogOpt) -> Result<(), IoError> {
    let file_path = opt.file_name;

    println!("dumping batch: {:#?}", file_path);
    let ft = print_logs(file_path);
    let result = run_block_on(ft);
    if let Err(err) = result {
        println!("error in async: {:#?}", err)
    };
    Ok(())
}

#[derive(Debug, StructOpt)]
pub(crate) struct IndexOpt {
    #[structopt(parse(from_os_str))]
    file_name: PathBuf,
}

pub(crate) fn dump_index(opt: IndexOpt) -> Result<(), IoError> {
    let file_path = opt.file_name;

    println!("dumping index: {:#?}", file_path);
    let ft = print_index(file_path);
    let result = run_block_on(ft);
    if let Err(err) = result {
        println!("error in async: {:#?}", err)
    };
    Ok(())
}

const MAX: u32 = 100;

async fn print_index(path: PathBuf) -> Result<(), StorageError> {
    let log = LogIndex::open_from_path(path).await?;

    println!("has {} bytes", log.len());
    let entries = log.len();
    let mut count: u32 = 0;
    let mut display: u32 = 0;
    for i in 0..entries {
        let (offset, pos) = log[i as usize].to_be();
        if offset > 0 && pos > 0 {
            count = count + 1;
            if count < MAX {
                println!("i: {} offset: {}  pos: {}", i, offset, pos);
                display = display + 1;
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

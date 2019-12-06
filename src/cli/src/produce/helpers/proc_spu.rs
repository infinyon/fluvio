//!
//! # Fluvio SPU Produce Log
//!
//! Connects to SPU server and sends the log.
//!

use std::io;
use std::io::prelude::*;
use std::net::SocketAddr;

use types::{print_cli_err, print_cli_ok};
use future_helper::run_block_on;

use crate::error::CliError;

use crate::produce::cli::RecordTouples;

use super::send_log_record_to_server;

// -----------------------------------
//  Fluvio SPU - Process Request
// -----------------------------------

/// Dispatch records based on the content of the record_touples variable
pub fn process_spu_produce_record(
    spu_addr: SocketAddr,
    topic: String,
    partition: i32,
    record_touples: RecordTouples,
    continous: bool,
) -> Result<(), CliError> {
    // in both cases, exit loop on error
    if record_touples.len() > 0 {
        // records from files
        for r_touple in record_touples {
            println!("{}", r_touple.0);
            process_record(spu_addr, topic.clone(), partition, r_touple.1);
        }
    } else {
        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            let text = line?;
            let record = text.as_bytes().to_vec();
            process_record(spu_addr, topic.clone(), partition, record);
            if !continous {
                return Ok(())
            }
        }
    }

    Ok(())
}

/// Process record and print success or error
/// TODO: Add version handling for SPU
fn process_record(spu_addr: SocketAddr, topic: String, partition: i32, record: Vec<u8>) {
    match run_block_on(send_log_record_to_server(
        spu_addr, topic, partition, record, None,
    )) {
        Ok(()) => print_cli_ok!(),
        Err(err) => print_cli_err!(format!("{}", err)),
    }
}

/*
#[cfg(test)]
mod tests {

    extern crate test;
    use std::net::SocketAddr;
    use std::io::Error as IoError;
    use std::io::ErrorKind;

    use test::Bencher;
    use super::process_record;
    fn create_sample_record() -> Vec<u8> {
        let mut record: Vec<u8> = vec![];
        for _ in 0..10000 {
            record.push(0x10);
        }
        record

    }


    #[bench]
    fn bench_spu_send_record(b: &mut Bencher) {
        let server_addr = "127.0.0.1:9004".to_owned();
        let socket_addr = server_addr
                .parse::<SocketAddr>()
                    .map_err(|err| IoError::new(ErrorKind::InvalidInput, format!("{}", err))).expect("kf server");
        b.iter(move || process_record(socket_addr,"test".to_owned(),0,create_sample_record()));
    }
}
*/

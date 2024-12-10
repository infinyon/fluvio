use std::{
    sync::Arc,
    path::{Path, PathBuf},
};

use chrono::Utc;
use flate2::{bufread::GzEncoder, Compression};
use fluvio_auth::root::RootAuthorization;
use fluvio_controlplane::spu_api::update_smartmodule::SmartModule;
use fluvio_controlplane_metadata::smartmodule::{
    SmartModuleSpec, SmartModuleWasm, SmartModuleWasmFormat,
};
use fluvio_protocol::{
    fixture::BatchProducer,
    record::{RecordData, Record, RecordSet, Batch, RawRecords},
    ByteBuf,
};
use fluvio_storage::{FileReplica, ReplicaStorage};

use crate::{core::GlobalContext, services::auth::SpuAuthGlobalContext};

use super::{create_public_server, SpuPublicServer};

mod stream_fetch;
mod produce;

/// create records that can be filtered
fn create_filter_records(records: u16) -> RecordSet {
    BatchProducer::builder()
        .records(records)
        .record_generator(Arc::new(generate_record))
        .build()
        .expect("batch")
        .records()
}

fn create_filter_raw_records(records: u16) -> RecordSet<RawRecords> {
    create_filter_records(records).try_into().expect("raw")
}

fn generate_record(record_index: usize, _producer: &BatchProducer) -> Record {
    let msg = match record_index {
        0 => "b".repeat(100),
        1 => "a".repeat(100),
        _ => "z".repeat(100),
    };
    Record::new(RecordData::from(msg))
}

fn vec_to_batch<T: AsRef<[u8]>>(records: &[T]) -> RecordSet {
    let mut batch = Batch::default();
    let header = batch.get_mut_header();
    header.magic = 2;
    header.producer_id = 1;
    header.producer_epoch = -1;
    header.first_timestamp = Utc::now().timestamp_millis();

    for (i, record_bytes) in records.iter().enumerate() {
        let mut record = Record::default();
        record.preamble.set_timestamp_delta(i as i64);
        let bytes: Vec<u8> = record_bytes.as_ref().to_owned();
        record.value = bytes.into();
        batch.add_record(record);
    }
    batch.get_mut_header().max_time_stamp = Utc::now().timestamp_millis();

    RecordSet::default().add(batch)
}

fn vec_to_raw_batch<T: AsRef<[u8]>>(records: &[T]) -> RecordSet<RawRecords> {
    vec_to_batch(records).try_into().expect("raw")
}

fn read_filter_from_path(filter_path: impl AsRef<Path>) -> Vec<u8> {
    let path = filter_path.as_ref();
    std::fs::read(path).unwrap_or_else(|_| panic!("Unable to read file {}", path.display()))
}

fn zip(raw_buffer: Vec<u8>) -> Vec<u8> {
    use std::io::Read;
    let mut encoder = GzEncoder::new(raw_buffer.as_slice(), Compression::default());
    let mut buffer = Vec::with_capacity(raw_buffer.len());
    encoder
        .read_to_end(&mut buffer)
        .unwrap_or_else(|_| panic!("Unable to gzip file"));
    buffer
}

fn read_wasm_module(module_name: &str) -> Vec<u8> {
    let spu_dir = std::env::var("CARGO_MANIFEST_DIR").expect("target");
    let wasm_path = PathBuf::from(spu_dir)
        .parent()
        .expect("parent")
        .parent()
        .expect("fluvio")
        .join(format!(
            "smartmodule/examples/target/wasm32-wasip1/release-lto/{module_name}.wasm"
        ));
    read_filter_from_path(wasm_path)
}

fn load_wasm_module<S: ReplicaStorage>(ctx: &GlobalContext<S>, module_name: &str) {
    let wasm = zip(read_wasm_module(module_name));
    ctx.smartmodule_localstore().insert(SmartModule {
        name: module_name.to_owned(),
        spec: SmartModuleSpec {
            wasm: SmartModuleWasm {
                format: SmartModuleWasmFormat::Binary,
                payload: ByteBuf::from(wasm),
            },
            ..Default::default()
        },
    });
}

fn create_public_server_with_root_auth(
    addr: String,
    ctx: Arc<GlobalContext<FileReplica>>,
) -> SpuPublicServer<RootAuthorization> {
    let auth_global_ctx =
        SpuAuthGlobalContext::new(ctx.clone(), Arc::new(RootAuthorization::new()));
    create_public_server(addr.to_owned(), auth_global_ctx.clone())
}

use fluvio_smartstream::{smartstream, SimpleRecord};

#[smartstream(map)]
pub fn my_map(_record: SimpleRecord) -> SimpleRecord {
    unimplemented!()
}

fn main() {}

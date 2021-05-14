use fluvio_smartstream::{smartstream, SimpleRecord};

#[smartstream(filter)]
pub fn my_filter(_record: &SimpleRecord) -> bool {
    unimplemented!()
}

fn main() {}

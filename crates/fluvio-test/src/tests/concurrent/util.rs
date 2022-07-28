use md5::Digest;
use tracing::instrument;

type Record = Vec<u8>;

#[instrument(level = "trace")]
pub fn rand_record() -> Record {
    let len: u16 = rand::random();
    let record: Vec<u8> = (0..len).map(|_| rand::random::<u8>()).collect();
    record
}

#[instrument(level = "trace")]
pub fn hash_messages(messages: &[String]) -> String {
    let mut hasher = md5::Md5::new();
    for m in messages.iter() {
        hasher.update(m);
    }
    format!("{:X?}", hasher.finalize())
}

#[instrument(level = "trace")]
pub fn hash_record(record: &[u8]) -> String {
    format!("{:X}", md5::Md5::digest(record))
}

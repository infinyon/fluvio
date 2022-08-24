use std::fmt;
use std::io::Error as IoError;
use std::io::ErrorKind;

use tracing::trace;
use bytes::BufMut;
use bytes::BytesMut;

use fluvio_future::file_slice::AsyncFileSlice;
use fluvio_protocol::store::FileWrite;
use fluvio_protocol::store::StoreValue;

use fluvio_protocol::bytes::Buf;
use fluvio_protocol::Decoder;
use fluvio_protocol::Encoder;
use fluvio_protocol::Version;

use crate::fetch::FetchRequest;

pub type FileFetchRequest = FetchRequest<FileRecordSet>;

#[derive(Default, Debug)]
pub struct FileRecordSet(AsyncFileSlice);

impl fmt::Display for FileRecordSet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "pos: {} len: {}", self.position(), self.len())
    }
}

#[allow(clippy::len_without_is_empty)]
impl FileRecordSet {
    pub fn position(&self) -> u64 {
        self.0.position()
    }

    pub fn len(&self) -> usize {
        self.0.len() as usize
    }

    pub fn raw_slice(&self) -> AsyncFileSlice {
        self.0.clone()
    }
}

impl From<AsyncFileSlice> for FileRecordSet {
    fn from(slice: AsyncFileSlice) -> Self {
        Self(slice)
    }
}

impl Encoder for FileRecordSet {
    fn write_size(&self, _version: Version) -> usize {
        self.len() + 4 // include header
    }

    fn encode<T>(&self, src: &mut T, version: Version) -> Result<(), IoError>
    where
        T: BufMut,
    {
        // can only encode zero length
        if self.len() == 0 {
            let len: u32 = 0;
            len.encode(src, version)
        } else {
            Err(IoError::new(
                ErrorKind::InvalidInput,
                format!("len {} is not zeo", self.len()),
            ))
        }
    }
}

impl Decoder for FileRecordSet {
    fn decode<T>(&mut self, _src: &mut T, _version: Version) -> Result<(), IoError>
    where
        T: Buf,
    {
        unimplemented!("file slice cannot be decoded in the ButMut")
    }
}

impl FileWrite for FileRecordSet {
    fn file_encode(
        &self,
        dest: &mut BytesMut,
        data: &mut Vec<StoreValue>,
        version: Version,
    ) -> Result<(), IoError> {
        // write total len
        let len: i32 = self.len() as i32;
        trace!("KfFileRecordSet encoding file slice len: {}", len);
        len.encode(dest, version)?;
        let bytes = dest.split_to(dest.len()).freeze();
        data.push(StoreValue::Bytes(bytes));
        data.push(StoreValue::FileSlice(self.raw_slice()));
        Ok(())
    }
}

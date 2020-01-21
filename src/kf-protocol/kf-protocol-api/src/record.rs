
use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::io::Error;
use std::io::ErrorKind;

use log::trace;
use content_inspector::{ContentType, inspect};


use kf_protocol::bytes::Buf;
use kf_protocol::bytes::BufMut;
use kf_protocol::bytes::BufExt;

use kf_protocol::Decoder;
use kf_protocol::DecoderVarInt;
use kf_protocol::Encoder;
use kf_protocol::EncoderVarInt;
use kf_protocol_derive::Decode;
use kf_protocol_derive::Encode;
use kf_protocol::Version;

use crate::Offset;
use crate::DefaultBatch;


pub type DefaultRecord = Record<DefaultAsyncBuffer>;

/// slice that can works in Async Context
pub trait AsyncBuffer {
    fn len(&self) -> usize;
}

pub trait Records {}

#[derive(Default)]
pub struct DefaultAsyncBuffer(Option<Vec<u8>>);

impl DefaultAsyncBuffer {
    pub fn new(val: Option<Vec<u8>>) -> Self {
        DefaultAsyncBuffer(val)
    }

    pub fn inner_value(self) -> Option<Vec<u8>> {
        self.0
    }

    pub fn inner_value_ref(&self) -> &Option<Vec<u8>> {
        &self.0
    }

    pub fn len(&self) -> usize {
        if self.0.is_some() {
            self.0.as_ref().unwrap().len()
        } else {
            0
        }
    }

    /// Check if value is binary content
    pub fn is_binary(&self) -> bool {
        if let Some(value) = self.inner_value_ref() {
            match inspect(value) {
                ContentType::BINARY => true,
                _ => false,
            }
        } else {
            false
        }
    }

    /// Describe value - return text, binary, or 0 bytes
    pub fn describe(&self) -> String {
        if self.inner_value_ref().is_some() {
            if self.is_binary() {
                format!("binary: ({} bytes)", self.len())
            } else {
                format!("text: '{}'", self)
            }
        } else {
            format!("empty: (0 bytes)")
        }
    }

}

impl From<Option<Vec<u8>>> for DefaultAsyncBuffer {
    fn from(val: Option<Vec<u8>>) -> Self {
        Self::new(val)
    }
}

impl AsyncBuffer for DefaultAsyncBuffer {
    fn len(&self) -> usize {
        match self.0 {
            Some(ref val) => val.len(),
            None => 0,
        }
    }
}

impl Debug for DefaultAsyncBuffer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.0 {
            Some(ref val) => write!(f, "{:?}", String::from_utf8_lossy(val)),
            None => write!(f, "no values"),
        }
    }
}

impl Display for DefaultAsyncBuffer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.0 {
            Some(ref val) => write!(f, "{}", String::from_utf8_lossy(val)),
            None => write!(f, ""),
        }
    }
}

impl From<String> for DefaultAsyncBuffer {
    fn from(value: String) -> Self {
        Self(Some(value.into_bytes()))
    }
}

impl From<Vec<u8>> for DefaultAsyncBuffer {
    fn from(value: Vec<u8>) -> Self {
        Self(Some(value))
    }
}

impl Encoder for DefaultAsyncBuffer {
    fn write_size(&self,_version: Version) -> usize {
        self.0.var_write_size()
    }

    fn encode<T>(&self, src: &mut T,_version: Version) -> Result<(), Error>
    where
        T: BufMut,
    {
        self.0.encode_varint(src)?;

        Ok(())
    }
}

impl Decoder for DefaultAsyncBuffer {
    fn decode<T>(&mut self, src: &mut T,_version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        trace!("decoding default asyncbuffer");
        self.0.decode_varint(src)?;
        trace!("value: {:#?}", self);
        Ok(())
    }
}

#[derive(Default, Debug)]
pub struct DefaultRecords {
    pub batches: Vec<DefaultBatch>,
}



impl fmt::Display for DefaultRecords {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,"{} batches",self.batches.len())
       
    }
}



impl DefaultRecords {
    pub fn add(mut self, batch: DefaultBatch) -> Self {
        self.batches.push(batch);
        self
    }
}

impl Decoder for DefaultRecords {
    fn decode<T>(&mut self, src: &mut T,version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        trace!("Decoding DefaultRecords");
        let mut len: i32 = 0;
        len.decode(src,version)?;
        trace!("recordsets len: {}", len);

        if src.remaining() < len as usize {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "not enough buf for batches",
            ));
        }

        let mut buf = src.take(len as usize);
        while buf.remaining() > 0 {
            trace!("decoding batches");
            let mut batch = DefaultBatch::default();
            batch.decode(&mut buf,version)?;
            self.batches.push(batch)
        }

        if buf.remaining() > 0 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "not enough buf for batches",
            ));
        }
        Ok(())
    }
}

impl Encoder for DefaultRecords {
    fn write_size(&self,version: Version) -> usize {
        self.batches
            .iter()
            .fold(4, |sum, val| sum + val.write_size(version))
    }

    fn encode<T>(&self, dest: &mut T,version: Version) -> Result<(), Error>
    where
        T: BufMut,
    {
        trace!("encoding Default Records");

        let mut out: Vec<u8> = Vec::new();

        for batch in &self.batches {
            trace!("decoding batch..");
            batch.encode(&mut out,version)?;
        }

        let length: i32 = out.len() as i32;
        trace!("recordset has {} bytes", length);
        length.encode(dest,version)?;

        dest.put_slice(&mut out);
        Ok(())
    }
}

#[derive(Decode, Encode, Default, Debug)]
pub struct RecordHeader {
    attributes: i8,
    #[varint]
    timestamp_delta: i64,
    #[varint]
    offset_delta: Offset,
}

impl RecordHeader {
    pub fn set_offset_delta(&mut self,delta: Offset) {
        self.offset_delta = delta;
    }
}

#[derive(Default)]
pub struct Record<B>
where
    B: Default,
{
    pub preamble: RecordHeader,
    pub key: B,
    pub value: B,
    pub headers: i64,
}

impl<B> Record<B>
where
    B: Default,
{
    pub fn get_offset_delta(&self) -> Offset {
        self.preamble.offset_delta
    }

    pub fn get_value(&self) -> &B {
        &self.value
    }

    pub fn value(self) -> B {
        self.value
    }
}

impl<B> Debug for Record<B>
where
    B: AsyncBuffer + Debug + Default,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "{:?}", &self.preamble)?;
        writeln!(f, "{:?}", &self.key)?;
        writeln!(f, "{:?}", &self.value)?;
        write!(f, "{:?}", &self.headers)
    }
}

impl<B> From<String> for Record<B>
where
    B: From<String> + Default,
{
    fn from(value: String) -> Self {
        let mut record = Record::default();
        record.value = value.into();
        record
    }
}

impl<B> From<Vec<u8>> for Record<B>
where
    B: From<Vec<u8>> + Default,
{
    fn from(value: Vec<u8>) -> Self {
        let mut record = Record::default();
        record.value = value.into();
        record
    }
}


impl<B> Encoder for Record<B>
where
    B: Encoder + Default,
{
    fn write_size(&self,version: Version) -> usize {
        let inner_size = self.preamble.write_size(version)
            + self.key.write_size(version)
            + self.value.write_size(version)
            + self.headers.var_write_size();
        let len: i64 = inner_size as i64;
        len.var_write_size() + inner_size
    }

    fn encode<T>(&self, dest: &mut T,version: Version) -> Result<(), Error>
    where
        T: BufMut,
    {
        let mut out: Vec<u8> = Vec::new();
        self.preamble.encode(&mut out,version)?;
        self.key.encode(&mut out,version)?;
        self.value.encode(&mut out,version)?;
        self.headers.encode_varint(&mut out)?;
        let len: i64 = out.len() as i64;
        trace!("record encode as {} bytes", len);
        len.encode_varint(dest)?;
        dest.put_slice(&out);
        Ok(())
    }
}

impl<B> Decoder for Record<B>
where
    B: Decoder,
{
    fn decode<T>(&mut self, src: &mut T,version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        trace!("decoding record");
        let mut len: i64 = 0;
        len.decode_varint(src)?;

        trace!("record contains: {} bytes", len);

        if (src.remaining() as i64) < len {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "not enought for record",
            ));
        }
        self.preamble.decode(src,version)?;
        trace!("offset delta: {}", self.preamble.offset_delta);
        self.key.decode(src,version)?;
        self.value.decode(src,version)?;
        self.headers.decode_varint(src)?;

        Ok(())
    }
}

#[cfg(test)]
mod test {

    use std::io::Cursor;
    use std::io::Error as IoError;

    use kf_protocol::Decoder;
    use kf_protocol::Encoder;

    use crate::DefaultRecord;

    #[test]
    fn test_decode_encode_record() -> Result<(), IoError> {
        let data = [
            0x14, // record length of 10
            0x00, // attributes
            0xea, 0x0e, // timestamp
            0x02, // offset delta, 1
            0x01, // key
            0x06, 0x64, 0x6f, 0x67, // value, 3 bytes len (dog)
            0x00, // 0 header
        ];

        let record = DefaultRecord::decode_from(&mut Cursor::new(&data),0)?;
        assert_eq!(record.as_bytes(0)?.len(), data.len());

        assert_eq!(record.write_size(0), data.len());
        assert_eq!(record.get_offset_delta(), 1);
        assert!(record.key.inner_value().is_none());
        let val = record.value.inner_value();
        assert!(val.is_some());
        let value = val.unwrap();
        assert_eq!(value.len(), 3);
        assert_eq!(value[0], 0x64);

        Ok(())
    }

}

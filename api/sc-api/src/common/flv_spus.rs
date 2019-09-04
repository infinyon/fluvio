//!
//! # Custom Spus
//!
//! Fields used by multiple Custom Spu APIs
//!
use std::io::{Error, ErrorKind};

use kf_protocol::Version;
use kf_protocol::{Decoder, Encoder};
use kf_protocol::bytes::{Buf, BufMut};
use kf_protocol::derive::{Encode, Decode};

// -----------------------------------
// Data Structures - FlvEndPointMetadata
// -----------------------------------

#[derive(Encode, Decode, Default, Debug)]
pub struct FlvEndPointMetadata {
    pub port: u16,
    pub host: String,
}

// -----------------------------------
// Data Structures - FlvRequestSpuType
// -----------------------------------

#[derive(Debug)]
pub enum FlvRequestSpuType {
    All,
    Custom,
}

// -----------------------------------
// Implementation - FlvRequestSpuType
// -----------------------------------
impl Default for FlvRequestSpuType {
    fn default() -> FlvRequestSpuType {
        FlvRequestSpuType::All
    }
}

impl Encoder for FlvRequestSpuType {
    // compute size
    fn write_size(&self, version: Version) -> usize {
        (0 as u8).write_size(version)
    }

    // encode match
    fn encode<T>(&self, dest: &mut T, version: Version) -> Result<(), Error>
    where
        T: BufMut,
    {
        // ensure buffer is large enough
        if dest.remaining_mut() < self.write_size(version) {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                format!(
                    "not enough capacity for spu request type {}",
                    self.write_size(version)
                ),
            ));
        }

        match self {
            FlvRequestSpuType::All => {
                let typ: u8 = 0;
                typ.encode(dest, version)?;
            }
            FlvRequestSpuType::Custom => {
                let typ: u8 = 1;
                typ.encode(dest, version)?;
            }
        }

        Ok(())
    }
}

impl Decoder for FlvRequestSpuType {
    fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        let mut value: u8 = 0;
        value.decode(src, version)?;
        match value {
            0 => *self = FlvRequestSpuType::All,
            1 => *self = FlvRequestSpuType::Custom,
            _ => {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    format!("invalid value for spu request type: {}", value),
                ))
            }
        }

        Ok(())
    }
}

// -----------------------------------
// Data Structures - FlvSpuType
// -----------------------------------

#[derive(Debug)]
pub enum FlvSpuType {
    Custom,
    Managed,
}

// -----------------------------------
// Implementation - FlvSpuType
// -----------------------------------
impl Default for FlvSpuType {
    fn default() -> FlvSpuType {
        FlvSpuType::Custom
    }
}

impl Encoder for FlvSpuType {
    // compute size
    fn write_size(&self, version: Version) -> usize {
        (0 as u8).write_size(version)
    }

    // encode match
    fn encode<T>(&self, dest: &mut T, version: Version) -> Result<(), Error>
    where
        T: BufMut,
    {
        // ensure buffer is large enough
        if dest.remaining_mut() < self.write_size(version) {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                format!(
                    "not enough capacity for spu type {}",
                    self.write_size(version)
                ),
            ));
        }

        match self {
            FlvSpuType::Custom => {
                let typ: u8 = 0;
                typ.encode(dest, version)?;
            }
            FlvSpuType::Managed => {
                let typ: u8 = 1;
                typ.encode(dest, version)?;
            }
        }

        Ok(())
    }
}

impl Decoder for FlvSpuType {
    fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        let mut value: u8 = 0;
        value.decode(src, version)?;
        match value {
            0 => *self = FlvSpuType::Custom,
            1 => *self = FlvSpuType::Managed,
            _ => {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    format!("invalid value for spu type: {}", value),
                ))
            }
        }

        Ok(())
    }
}

// -----------------------------------
// Data Structures - FlvSpuResolution
// -----------------------------------

#[derive(Debug)]
pub enum FlvSpuResolution {
    Online,
    Offline,
    Init,
}

// -----------------------------------
// Implementation - FlvSpuResolution
// -----------------------------------
impl Default for FlvSpuResolution {
    fn default() -> FlvSpuResolution {
        FlvSpuResolution::Init
    }
}

impl Encoder for FlvSpuResolution {
    // compute size
    fn write_size(&self, version: Version) -> usize {
        (0 as u8).write_size(version)
    }

    // encode match
    fn encode<T>(&self, dest: &mut T, version: Version) -> Result<(), Error>
    where
        T: BufMut,
    {
        // ensure buffer is large enough
        if dest.remaining_mut() < self.write_size(version) {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                format!(
                    "not enough capacity for spu resolution {}",
                    self.write_size(version)
                ),
            ));
        }

        match self {
            FlvSpuResolution::Online => {
                let typ: u8 = 0;
                typ.encode(dest, version)?;
            }
            FlvSpuResolution::Offline => {
                let typ: u8 = 1;
                typ.encode(dest, version)?;
            }
            FlvSpuResolution::Init => {
                let typ: u8 = 2;
                typ.encode(dest, version)?;
            }
        }

        Ok(())
    }
}

impl Decoder for FlvSpuResolution {
    fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        let mut value: u8 = 0;
        value.decode(src, version)?;
        match value {
            0 => *self = FlvSpuResolution::Online,
            1 => *self = FlvSpuResolution::Offline,
            2 => *self = FlvSpuResolution::Init,
            _ => {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    format!("invalid value for spu resolution: {}", value),
                ))
            }
        }

        Ok(())
    }
}

// -----------------------------------
// Data Structures - FlvSpuGroupResolution
// -----------------------------------

#[derive(Debug)]
pub enum FlvSpuGroupResolution {
    Init,
    Invalid,
    Reserved,
}

// -----------------------------------
// Implementation - FlvSpuGroupResolution
// -----------------------------------
impl Default for FlvSpuGroupResolution {
    fn default() -> FlvSpuGroupResolution {
        FlvSpuGroupResolution::Init
    }
}

impl Encoder for FlvSpuGroupResolution {
    // compute size
    fn write_size(&self, version: Version) -> usize {
        (0 as u8).write_size(version)
    }

    // encode match
    fn encode<T>(&self, dest: &mut T, version: Version) -> Result<(), Error>
    where
        T: BufMut,
    {
        // ensure buffer is large enough
        if dest.remaining_mut() < self.write_size(version) {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                format!(
                    "not enough capacity for group spu resolution {}",
                    self.write_size(version)
                ),
            ));
        }

        match self {
            FlvSpuGroupResolution::Init => {
                let typ: u8 = 0;
                typ.encode(dest, version)?;
            }
            FlvSpuGroupResolution::Invalid => {
                let typ: u8 = 1;
                typ.encode(dest, version)?;
            }
            FlvSpuGroupResolution::Reserved => {
                let typ: u8 = 2;
                typ.encode(dest, version)?;
            }
        }

        Ok(())
    }
}

impl Decoder for FlvSpuGroupResolution {
    fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        let mut value: u8 = 0;
        value.decode(src, version)?;
        match value {
            0 => *self = FlvSpuGroupResolution::Init,
            1 => *self = FlvSpuGroupResolution::Invalid,
            2 => *self = FlvSpuGroupResolution::Reserved,
            _ => {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    format!("invalid value for group spu resolution: {}", value),
                ))
            }
        }

        Ok(())
    }
}

// -----------------------------------
// Data Structures - CustomSpu
// -----------------------------------

#[derive(Debug)]
pub enum FlvCustomSpu {
    Name(String),
    Id(i32),
}

// -----------------------------------
// Implementation - CustomSpu
// -----------------------------------
impl Default for FlvCustomSpu {
    fn default() -> FlvCustomSpu {
        FlvCustomSpu::Name("".to_string())
    }
}

impl Encoder for FlvCustomSpu {
    // compute size
    fn write_size(&self, version: Version) -> usize {
        let type_size = (0 as u8).write_size(version);
        match self {
            FlvCustomSpu::Name(name) => type_size + name.write_size(version),
            FlvCustomSpu::Id(id) => type_size + id.write_size(version),
        }
    }

    // encode match
    fn encode<T>(&self, dest: &mut T, version: Version) -> Result<(), Error>
    where
        T: BufMut,
    {
        // ensure buffer is large enough
        if dest.remaining_mut() < self.write_size(version) {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                format!(
                    "not enough capacity for custom spu len of {}",
                    self.write_size(version)
                ),
            ));
        }

        match self {
            FlvCustomSpu::Name(name) => {
                let typ: u8 = 0;
                typ.encode(dest, version)?;
                name.encode(dest, version)?;
            }
            FlvCustomSpu::Id(id) => {
                let typ: u8 = 1;
                typ.encode(dest, version)?;
                id.encode(dest, version)?;
            }
        }

        Ok(())
    }
}

impl Decoder for FlvCustomSpu {
    fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), Error>
    where
        T: Buf,
    {
        let mut value: u8 = 0;
        value.decode(src, version)?;
        match value {
            0 => {
                let mut name: String = String::default();
                name.decode(src, version)?;
                *self = FlvCustomSpu::Name(name)
            }
            1 => {
                let mut id: i32 = 0;
                id.decode(src, version)?;
                *self = FlvCustomSpu::Id(id)
            }
            _ => {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    format!("invalid value for Custom Spu: {}", value),
                ))
            }
        }

        Ok(())
    }
}

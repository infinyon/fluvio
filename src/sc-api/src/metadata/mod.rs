mod update_all;
mod update_replica;
mod update_spu;

pub use update_all::*;
pub use update_replica::*;
pub use update_spu::*;
pub use request::*;

mod request {

    use std::io::Error;
    use std::io::ErrorKind;

    use log::trace;
    use kf_protocol::derive::Decode;
    use kf_protocol::derive::Encode;
    use kf_protocol::api::Request;
    use kf_protocol::Version;
    use kf_protocol::Encoder;
    use kf_protocol::Decoder;
    use kf_protocol::bytes::{Buf, BufMut};

    use crate::AdminPublicApiKey;
    use super::*;

    #[derive(Decode, Encode, Debug)]
    pub struct WatchMetadataRequest {
        /// number of milliseconds between refresh
        pub re_sync_period_ms: u16,
    }

    impl Default for WatchMetadataRequest {
        fn default() -> Self {
            Self {
                re_sync_period_ms: 6000, // 60 seconds
            }
        }
    }

    impl Request for WatchMetadataRequest {
        const API_KEY: u16 = AdminPublicApiKey::WatchMetadata as u16;
        type Response = WatchMetadataResponse;
    }

    #[derive(Debug)]
    pub enum WatchMetadataResponse {
        All(UpdateAllMetadataResponse),
        Replica(UpdateReplicaResponse),
        SPU(UpdateSpuResponse),
    }

    impl Default for WatchMetadataResponse {
        fn default() -> Self {
            Self::All(UpdateAllMetadataResponse::default())
        }
    }

    impl std::fmt::Display for WatchMetadataResponse {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            match self {
                Self::All(response) => write!(f, "Watch All {}", response),
                Self::Replica(response) => write!(f, "Watch Replica {}", response),
                Self::SPU(response) =>  write!(f,"Watch SPU: {}",response)
            }
        }
    }



    impl Encoder for WatchMetadataResponse {
        fn write_size(&self, version: Version) -> usize {
            let type_size = (0 as u8).write_size(version);
            let total_size = type_size + match self {
                    Self::All(response) => response.write_size(version),
                    Self::Replica(response) => response.write_size(version),
                    Self::SPU(response) => response.write_size(version),
                };
            trace!("WatchMetadataResponse: write size {} bytes", total_size);
            return total_size;
        }

        fn encode<T>(&self, dest: &mut T, version: Version) -> Result<(), Error>
        where
            T: BufMut,
        {
            if dest.remaining_mut() < self.write_size(version) {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    format!(
                        "should have {} bytes but only {} in buffer for UpdateMedataResponse",
                        self.write_size(version),
                        dest.remaining_mut()
                    ),
                ));
            }

            match self {
                Self::All(response) => {
                    let typ: u8 = 0;
                    trace!("encoding WatchMetadataResponse All");
                    typ.encode(dest, version)?;
                    response.encode(dest, version)?;
                }

                Self::Replica(response) => {
                    let typ: u8 = 1;
                    trace!("encoding WatchMetadataResponse Replica");
                    typ.encode(dest, version)?;
                    response.encode(dest, version)?;
                }

                Self::SPU(response) => {
                    let typ: u8 = 2;
                    trace!("encoding WatchMetadataResponse SPU");
                    typ.encode(dest, version)?;
                    response.encode(dest, version)?;
                }
            }

            Ok(())
        }
    }

    impl Decoder for WatchMetadataResponse {
        fn decode<T>(&mut self, src: &mut T, version: Version) -> Result<(), Error>
        where
            T: Buf,
        {
            let mut typ: u8 = 0;
            typ.decode(src, version)?;
            trace!("WatchMetadataResponse decoding type: {}", typ);

            match typ {
                0 => {
                    trace!("WatchMetadataResponse decoding UpdateAllMetadataResponse");
                    let mut response = UpdateAllMetadataResponse::default();
                    response.decode(src, version)?;
                    *self = Self::All(response);
                    Ok(())
                }

                1 => {
                    trace!("WatchMetadataResponse decoding UpdateReplicaResponse");
                    let mut response = UpdateReplicaResponse::default();
                    response.decode(src, version)?;
                    *self = Self::Replica(response);
                    Ok(())
                }

                2 => {
                    trace!("WatchMetadataResponse decoding UpdateSpuResponse");
                    let mut response = UpdateSpuResponse::default();
                    response.decode(src, version)?;
                    *self = Self::SPU(response);
                    Ok(())
                }

                // Unexpected type
                _ => Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("unknown WatchMetadataResponse type {}", typ),
                )),
            }
        }
    }
}

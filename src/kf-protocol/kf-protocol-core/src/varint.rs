// varint decoder
// <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>.
// also kafka: ByteUtils.java

use std::io::Error;
use std::io::ErrorKind;

use log::trace;
use bytes::Buf;
use bytes::BufMut;



// zigzag decoding
pub fn varint_decode<T>(buf: &mut T) -> Result<(i64,usize),Error> where T:Buf {

    let mut num: i64 = 0;
    let mut shift: usize = 0;

    loop {
        if buf.remaining() == 0 {
            return Err(Error::new(ErrorKind::UnexpectedEof,"varint decoding no more bytes left"));
        }

        let b = buf.get_u8();
        trace!("var byte: {:#X}",b);

        num |= ((b & 0x7f) as i64) << shift;
        shift += 7;

        if b & 0x80 == 0 {
            break;
        }
        
    }

    Ok(( (num >> 1) ^ - (num & 1),shift/7))
}

// store varaint
pub fn variant_encode<T>(buf: &mut T,num: i64) -> Result<(),Error> where T:BufMut {

    let mut v = (num << 1) ^ (num >> 31);

    while (v & 0xffffff80) != 0  {
        let b: u8 = (( v & 0x7f) | 0x80) as u8;
        if buf.remaining_mut() == 0 {
             return Err(Error::new(ErrorKind::UnexpectedEof,"varint encoding no more bytes left"));
        }
        buf.put_u8(b);
        v >>= 7;
    }
    if buf.remaining_mut() == 0 {
        return Err(Error::new(ErrorKind::UnexpectedEof,"varint encoding no more bytes left"));
    }
    buf.put_u8(v as u8);
    Ok(())
}

pub fn variant_size(num: i64) -> usize {

    let mut v = (num << 1) ^ (num >> 31);
    let mut bytes = 1;

    while (v & 0xffffff80) != 0  {
        bytes += 1;
        v >>= 7;
    } 

    bytes
}





#[cfg(test)]
mod test {

    use std::io::Cursor;
    use bytes::{BytesMut, BufMut};
    use super::varint_decode;
    use super::variant_encode;
    use super::variant_size;


    #[test] 
    fn test_varint_decode_with_test_set() {
        let test_set = vec![
            (0, vec![0x00]),
            (-1, vec![0x1]),
            (1, vec![0x2]),
            (63, vec![0x7e]),
            (7, vec![14]),
            (10, vec![0x14]),
            (4, vec![08]),
            (8191,vec![0xfe,0x7f]),
            (-134217729, vec![0x81, 0x80, 0x80, 0x80, 0x01])
        ];
        
        for (expected,input) in test_set {
            let mut buf = BytesMut::with_capacity(1024);
            buf.put_slice(&input);

            let mut src = Cursor::new(&buf);
            let result = varint_decode(&mut src);
            assert!(result.is_ok());
            let (value,shift) = result.unwrap();
            assert_eq!(value,expected);
            assert_eq!(shift,input.len());
        }
        
    }


    #[test]
    fn test_varint_encode_with_test_set() {
        let test_set = vec![
            (0, vec![0x00]),
            (-1, vec![0x1]),
            (1, vec![0x2]),
            (63, vec![0x7e]),
            (7, vec![14]),
            (10, vec![0x14]),
            (4, vec![08]),
            (8191,vec![0xfe,0x7f]),
            (-134217729, vec![0x81, 0x80, 0x80, 0x80, 0x01])
        ];
        
        for (input,output) in test_set {
            let mut src = vec![];
            let result = variant_encode(&mut src,input);
            assert!(result.is_ok());
            assert_eq!(src.len(),output.len());
            assert_eq!(variant_size(input),output.len());
            for i in 0..src.len() {
                assert_eq!(src[i],output[i]);
            }
        }
        
    }







    
}
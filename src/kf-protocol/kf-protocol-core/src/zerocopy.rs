#[cfg(test)]
mod test {

    use std::slice::from_raw_parts;
    use std::mem::size_of;

    //use super::BigU32;

    #[repr(C)]
    struct Buffer {
        i1: u16,
        i2: u16
    }

    // doing basic test of big integration conversion and so forth
    #[test]
    fn test_zero_copy() {

        let b = Buffer {
            i1: (10 as u16).to_be(),
            i2: 11
        };

        let p: *const Buffer = &b;
        let p: *const u8 = p as *const u8;
        let bytes = unsafe { from_raw_parts(p,size_of::<Buffer>())};

        println!("{:X}{:X}{:X}{:X}",bytes[0],bytes[1],bytes[2],bytes[3]);   

        // should print out as 0AB0  

        let z: *const Buffer = p as *const Buffer;
        let k: &Buffer = unsafe { &*z};
        assert_eq!(k.i2,11);
        assert_eq!(k.i1.to_be(),10);
    }

  
}
pub use fluvio_dataplane_protocol as dataplane;
pub use dataplane::record::{Record, RecordData};

#[cfg(feature = "derive")]
pub use fluvio_smartstream_derive::smartstream;

pub use eyre::Error;
pub type Result<T> = eyre::Result<T>;

pub mod memory {
    /// Allocate memory into the module's linear memory
    /// and return the offset to the start of the block.
    #[no_mangle]
    pub fn alloc(len: usize) -> *mut u8 {
        // create a new mutable buffer with capacity `len`
        let mut buf = Vec::with_capacity(len);
        // take a mutable pointer to the buffer
        let ptr = buf.as_mut_ptr();
        // take ownership of the memory block and
        // ensure the its destructor is not
        // called when the object goes out of scope
        // at the end of the function
        std::mem::forget(buf);
        // return the pointer so the runtime
        // can write data at this offset
        ptr
    }

    #[no_mangle]
    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn dealloc(ptr: *mut u8, size: usize) {
        let data = Vec::from_raw_parts(ptr, size, size);

        std::mem::drop(data);
    }
}

use wasmtime::*;

use anyhow::{Result, Error, anyhow};

const ALLOC_FN: &str = "alloc";
const MEMORY: &str = "memory";
// const ARRAY_SUM_FN: &str = "array_sum";
// const UPPER_FN: &str = "upper";
// const DEALLOC_FN: &str = "dealloc";

/// Copy a byte array into an instance's linear memory
/// and return the offset relative to the module's memory.
pub(crate) fn copy_memory_to_instance(
    store: &mut impl AsContextMut,
    instance: &Instance,
    bytes: &[u8],
) -> Result<isize, Error> {
    // Get the "memory" export of the module.
    // If the module does not export it, just panic,
    // since we are not going to be able to copy the data.
    let memory = instance
        .get_memory(&mut *store, MEMORY)
        .ok_or_else(|| anyhow!("Missing memory"))?;

    // The module is not using any bindgen libraries,
    // so it should export its own alloc function.
    //
    // Get the guest's exported alloc function, and call it with the
    // length of the byte array we are trying to copy.
    // The result is an offset relative to the module's linear memory,
    // which is used to copy the bytes into the module's memory.
    // Then, return the offset.
    let alloc = instance
        .get_func(&mut *store, ALLOC_FN)
        .ok_or_else(|| anyhow!("missing alloc"))?;

    let mut alloc_result = [Val::I32(0)];
    alloc.call(
        &mut *store,
        &[Val::from(bytes.len() as i32)],
        &mut alloc_result,
    )?;

    let guest_ptr_offset = match alloc_result
        .first()
        .ok_or_else(|| anyhow!("missing alloc"))?
    {
        Val::I32(val) => *val as isize,
        _ => return Err(Error::msg("guest pointer must be Val::I32")),
    };
    unsafe {
        let raw = memory.data_ptr(store).offset(guest_ptr_offset);
        raw.copy_from(bytes.as_ptr(), bytes.len());
    }

    Ok(guest_ptr_offset)
}

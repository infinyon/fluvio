// memory mapped file

use std::fs::OpenOptions;
use std::io::Error as IoError;
use std::path::Path;

use memmap::Mmap;
use memmap::MmapMut;

use crate::asyncify;
use crate::fs::AsyncFile;

/// memory mapped file for writing
pub struct MemoryMappedMutFile(pub MmapMut);

impl MemoryMappedMutFile {


    pub async fn create<'a>(m_path: &'a Path, len: u64) -> Result<(Self, AsyncFile), IoError>
    {
        let (m_map, mfile) = asyncify (move || {
            let mfile = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(m_path)
                .unwrap();

            mfile.set_len(len)?;
            
            unsafe { MmapMut::map_mut(&mfile) }.map(|mm_file| (mm_file, mfile))
        }).await?;

        Ok((
            MemoryMappedMutFile::from_mmap(m_map),
            AsyncFile::from_std(mfile,m_path.to_owned()),
        ))
    }


    fn from_mmap(mmap: MmapMut) -> MemoryMappedMutFile {
        MemoryMappedMutFile(mmap)
    }

    // get memory file
    pub fn get_mut_mem_file(&mut self) -> &mut MmapMut {
        &mut self.0
    }

    /// write bytes at location,
    /// return number bytes written
    pub fn write_bytes(&mut self, pos: usize, bytes: &Vec<u8>) {
        let m_file = self.get_mut_mem_file();
        let m_array = &mut m_file[..];
        for i in 0..bytes.len() {
            m_array[i + pos] = bytes[i];
        }
    }

    pub async fn flush_ft(&self) -> Result<(),IoError> {
        asyncify(move || self.0.flush()).await
    }

    pub async fn flush_async_ft(&self) ->  Result<(), IoError> {
        asyncify(move || self.0.flush_async()).await
    }

    pub async fn flush_range_ft(
        &self,
        offset: usize,
        len: usize,
    ) -> Result<(), IoError> {
        asyncify(move || self.0.flush_range(offset, len)).await
    }
}

/// memory mmapped file for reading
pub struct MemoryMappedFile(pub Mmap);

impl MemoryMappedFile {

    /// open memory file, specify minimum size 
    pub async fn open<P>(path: P,min_len: u64) -> Result<(Self, AsyncFile), IoError>
    where
        P: AsRef<Path> + Send + 'static,
    {
        let m_path = path.as_ref();
        let (m_map, mfile) = asyncify(|| {
            let mfile = OpenOptions::new().read(true).open(m_path).unwrap();
            let meta = mfile.metadata().unwrap();
            if meta.len() == 0 {
                    mfile.set_len(min_len)?;
            }

            unsafe { Mmap::map(&mfile) }.map(|mm_file| (mm_file, mfile))
        }).await?;

        Ok((
            MemoryMappedFile::from_mmap(m_map),
            AsyncFile::from_std(mfile,m_path.to_owned()),
        ))
    }

    fn from_mmap(mmap: Mmap) -> MemoryMappedFile {
        MemoryMappedFile(mmap)
    }

    // get memory file
    pub fn get_mem_file(&mut self) -> &mut Mmap {
        &mut self.0
    }
}

#[cfg(test)]
mod tests {

    use std::env::temp_dir;
    use std::fs::File;
    use std::io::Error as IoError;
    use std::io::Read;

    use future_helper::test_async;

    use super::MemoryMappedMutFile;
    use utils::fixture::ensure_clean_file;

    #[test_async]
    async fn test_mmap_write_slice() -> Result<(),IoError> {
       
        let index_path = temp_dir().join("test.index");
        ensure_clean_file(&index_path.clone());

        let result = MemoryMappedMutFile::create(&index_path,3).await;
        assert!(result.is_ok());

        let (mut mm_file, _) = result.unwrap();

        let mm = mm_file.get_mut_mem_file();
        let src = [0x01, 0x02, 0x03];
        mm.copy_from_slice(&src);

        mm_file.flush_ft().await?;

        let mut f = File::open(&index_path)?;
        let mut buffer = vec![0; 3];
        f.read(&mut buffer)?;
        assert_eq!(buffer[0], 0x01);
        assert_eq!(buffer[1], 0x02);
        assert_eq!(buffer[2], 0x03);

        Ok(()) 
    }

    #[test_async]
    async fn test_mmap_write_pair_slice() -> Result<(),IoError> {
     
        let index_path = temp_dir().join("pairslice.index");
        ensure_clean_file(&index_path.clone());

        let result = MemoryMappedMutFile::create(&index_path, 24).await;
        assert!(result.is_ok());

        let (mut mm_file, _) = result.unwrap();

        let mm = mm_file.get_mut_mem_file();
        let src: [(u32, u32); 3] = [(5, 10), (11, 22), (50, 100)];
        let (_, bytes, _) = unsafe { src.align_to::<u8>() };
        assert_eq!(bytes.len(), 24);

        mm.copy_from_slice(&bytes);

        mm_file.flush_ft().await?;

        let (mut mm_file2, _) =
            MemoryMappedMutFile::create(&index_path, 24).await?;
        let mm2 = mm_file2.get_mut_mem_file();
        let (_, pairs, _) = unsafe { mm2.align_to::<(u32, u32)>() };
        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0].0, 5);
        assert_eq!(pairs[2].1, 100);

        Ok(())
              
    }

    #[test_async]
    async fn test_mmap_write_with_pos() -> Result<(),IoError> {
        
        let index_path = temp_dir().join("testpos.index");
        ensure_clean_file(&index_path.clone());

        let (mut mm_file, _) = MemoryMappedMutFile::create(&index_path, 10).await?;

        let src = vec![0x05, 0x10, 0x44];
        mm_file.write_bytes(5, &src);

        mm_file.flush_ft().await?;

        let mut f = File::open(&index_path)?;
        let mut buffer = vec![0; 10];
        f.read(&mut buffer)?;
        assert_eq!(buffer[5], 0x05);
        assert_eq!(buffer[6], 0x10);
        assert_eq!(buffer[7], 0x44);

        Ok(())
    }

    /*
    use std::fs::OpenOptions;
    use std::path::PathBuf;
    use memmap::MmapMut;


    #[test]
    fn debug_kafka_inspect() -> io::Result<()>  {

        let path =  "/tmp/kafka-logs/test-0/00000000000000000000.index";
        let file = OpenOptions::new()
                       .read(true)
                       .write(true)
                       .open(path)?;

        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        println!("file size: {}",mmap.len());
        Ok(())
    }

    #[test]
    fn debug_file_inspect() -> io::Result<()>  {

        let path =  "/tmp/kafka-logs/test-0/00000000000000000000.index";
        let file = File::open(path)?;
        let metadata = file.metadata()?;

        println!("file len: {:#?}",metadata.len());
        Ok(())
    }
    */

}

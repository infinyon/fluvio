pub mod local;


pub mod spu {

    use std::error::Error;

    pub trait SpuTarget {
        type RunTimeError: Error;

        /// starts spu process
        fn start(&self) -> Result<(), Self::RunTimeError>;

        /// stop spu process
        fn stop(&self) -> Result<(), Self::RunTimeError>;
    }

    /// manages spu
    pub trait SpuClusterManager {

        type Spu: SpuTarget;

        /// create new spu target
        fn create_spu_absolute(&self, id: u16) -> Self::Spu; 

        /// create spu with relative (0) from some base
        fn create_spu_relative(&self, id: u16) -> Self::Spu; 

    }




}
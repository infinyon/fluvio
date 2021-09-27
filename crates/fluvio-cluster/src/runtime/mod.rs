pub mod local;

pub mod spu {

    use anyhow::Result;

    use fluvio_controlplane_metadata::spu::SpuSpec;
    use fluvio_types::SpuId;

    pub trait SpuTarget: Sync + Send {
        fn id(&self) -> SpuId;

        fn spec(&self) -> &SpuSpec;

        /// starts spu process
        fn start(&self) -> Result<()>;

        /// stop spu process
        fn stop(&self) -> Result<()>;
    }

    /// manages spu
    pub trait SpuClusterManager {
        /// create new spu target
        fn create_spu_absolute(&self, id: u16) -> Box<dyn SpuTarget>;

        /// create spu with relative (0) from some base
        fn create_spu_relative(&self, id: u16) -> Box<dyn SpuTarget>;

        fn terminate_spu(&self, id: SpuId) -> Result<()>;
    }
}

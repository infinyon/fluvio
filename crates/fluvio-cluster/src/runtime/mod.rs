pub mod local;


pub mod spu {

        /// manages spu
        pub trait SpuClusterManager {

            /// start spu
            fn start_spu(&self, id: u16);
    
            /// stop spu
            fn terminate_spu(&self, id: u16);
    
        }

}
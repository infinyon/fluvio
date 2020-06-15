mod controller;

pub use inner::SpuClient;



mod inner {


    use crate::Client;

    pub struct SpuClient(Client);

    impl SpuClient {
        #[allow(unused)]
        fn new(client: Client) -> Self {
            Self(client)
        }

        pub fn mut_client(&mut self) -> &mut Client {
            &mut self.0
        }
    }
}

mod config {



}


mod manager {
    
    use log::warn;
    use dashmap::DashMap;

    use flv_types::SpuId;
    use kf_protocol::api::ReplicaKey;
    use kf_protocol::api::Offset;

    use crate::ScClient;

    /// index of spu to offsets
    pub struct ReplicaOffset {
        spu: SpuId,
        replica: ReplicaKey,
        hw: Offset,
        leo: Offset
    }

    pub struct Manager {
        controller: ScClient,
        offsets: DashMap<ReplicaKey,ReplicaOffset>
    }


    impl Manager {

        pub fn new(controller: ScClient) -> Self {
            Self {
                controller,
                offsets: DashMap::new()
            }
        }

        pub async fn fetch() {
            
        }

        pub fn insert_offset(&mut self,replica: ReplicaKey, offset: ReplicaOffset) {
            self.offsets.insert(replica,offset);
        }

        pub fn update_offset(&mut self,replica: &ReplicaKey, hw: Offset, leo: Offset) {
            if let Some(mut offset) = self.offsets.get_mut(replica) {
                let value = offset.value_mut();
                value.hw = hw;
                value.leo = leo;
            } else {
                warn!("no replica: {} ",replica);
            }
        }
    }


    
}
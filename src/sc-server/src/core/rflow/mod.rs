mod store;
mod rflow;


pub use store::MemStore;

pub trait FlowSpec{

    type Key;
}

pub trait FlowStatus{}




/// interface to metadata backend
///
pub trait KVService {
    

    //fn watch<S,P>(&self) -> ResponseFuture
}
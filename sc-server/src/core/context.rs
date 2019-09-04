//!
//! #  SC Context
//!
//! Streaming Controller Context stores entities that persist through system operation.
//!

use std::sync::Arc;


//use super::send_channels::ScSendChannels;

#[derive(Debug)]
pub struct ScContext<C> {
   // send_channels: ScSendChannels,
    conn_manager: Arc<C>,
}

impl <C>Clone for ScContext<C> {

    fn clone(&self) -> Self {
        ScContext {
      //      send_channels: self.send_channels.clone(),
            conn_manager: self.conn_manager.clone()
        }
    }
}

// -----------------------------------
// Global Context - Implementation
// -----------------------------------

impl <C>ScContext<C> where C: SpuConnections  {

    pub fn new(conn_manager: Arc<C>) -> Self {
        Self { conn_manager}
    }

    // reference to connection metadata
    pub fn conn_manager(&self) -> Arc<C> {
        self.conn_manager.clone()
    }

     
}

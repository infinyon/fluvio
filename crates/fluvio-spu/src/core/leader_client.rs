use std::fmt::Debug;

use fluvio::Fluvio;

/// maintain connections to all leaders
#[derive(Debug)]
pub struct LeaderConnections {}

impl LeaderConnections {
    pub fn new() -> Self {
        LeaderConnections {}
    }

    pub async fn get_connection(&self) -> Fluvio {
        todo!()
    }

    
}

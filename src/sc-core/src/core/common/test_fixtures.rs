use flv_metadata::message::*;

// Test Actions - helps generate composite actions
pub enum TAction {
    MOD,
    DEL,
    UPDATE,
}

impl From<TAction> for MsgType {
    fn from(value: TAction) -> Self {
        match value {
            TAction::UPDATE => MsgType::UPDATE,
            TAction::DEL => MsgType::DELETE,
            TAction::MOD => MsgType::UPDATE,
        }
    }
}

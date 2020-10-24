pub mod basic;

#[cfg(test)]
mod test;

pub use common::*;

mod common {

    use async_trait::async_trait;

    use fluvio_auth::AuthContext;
    use fluvio_controlplane_metadata::core::Spec;

    use crate::core::SharedContext;

    //pub type ScAdminRequest = (Action, Object, Option<ObjectName>);

    /// Auth Service Context, this hold individual context that is enough enforce auth
    /// for this service context
    pub struct AuthServiceContext {
        pub global_ctx: SharedContext,
        pub auth: Box<dyn AuthContext>,
    }
    
    impl AuthServiceContext {
    
        pub fn new(global_ctx: SharedContext, auth: Box<dyn AuthContext>) -> Self {
            Self {
                global_ctx,
                auth
            }
        }
    }
    
}


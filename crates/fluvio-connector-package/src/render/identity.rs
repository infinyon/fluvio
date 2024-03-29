
pub struct IdentityStore<'a> {
    context_name: &'a str,
}

impl<'a> IdentityStore<'a> {
    pub fn new(context_name: &'a str) -> Self {
        Self {
            context_name,
        }
    }
}

pub use fluvio_controlplane_metadata::table::*;

mod convert {

    use std::io::Error;
    use std::io::ErrorKind;
    use std::convert::TryInto;

    use crate::objects::CreateType;
    use crate::{AdminSpec,NameFilter};
    use super::*;

    impl AdminSpec for TableSpec {
        const AdminType: u8 = CreateType::Table;

        type ListFilter = NameFilter;

        type DeleteKey = String;
    }

  
}

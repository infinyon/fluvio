mod spec;
mod status;

pub use self::spec::AuthTokenSpec;
pub use self::spec::TokenType;
pub use self::status::AuthTokenStatus;
pub use self::status::TokenResolution;

use metadata_core::Crd;
use metadata_core::CrdNames;
use metadata_core::GROUP;
use metadata_core::V1;

const AUTH_TOKEN_API: Crd = Crd {
    group: GROUP,
    version: V1,
    names: CrdNames {
        kind: "AuthToken",
        plural: "auth-tokens",
        singular: "auth-token",
    },
};

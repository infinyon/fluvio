use std::fs::File;
use std::path::PathBuf;
use std::convert::TryFrom;
use std::collections::HashMap;

use super::basic::{Action, Object, Policy};

#[test]
fn test_policy_serialization() {
    let mut policy = Policy::default();

    let mut default_role = HashMap::new();

    default_role.insert(Object::Topic, vec![Action::All]);
    default_role.insert(Object::Partition, vec![Action::All]);
    default_role.insert(Object::SpuGroup, vec![Action::Read]);
    default_role.insert(Object::CustomSpu, vec![Action::Read]);
    default_role.insert(Object::Spu, vec![Action::Read]);

    policy.0.insert(String::from("Default"), default_role);

    let tmp_file_path = PathBuf::from("/tmp/policy.json");
    let tmp = File::create(tmp_file_path.clone()).expect("failed to create policy file");
    serde_json::to_writer(&tmp, &policy).expect("failed to serialize policy to json file");

    let recovered_policy =
        Policy::try_from(tmp_file_path).expect("failed to parse policy from file");

    assert_eq!(
        policy, recovered_policy,
        "serialized and deserialized policies from file should match"
    )
}

#[test]
fn test_policy_enforcement() {
    // let identity =
    // let auth = AuthorizationTest::create_authorization_context(identity: Self::IdentityContext, config: Policy)
}

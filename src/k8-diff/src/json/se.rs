use serde::Serialize;
use serde_json::Value;

use super::PatchObject;
use crate::Diff;

impl Serialize for PatchObject {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ::serde::Serializer,
    {
        let diff_maps = self.get_inner_ref();
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(diff_maps.len()))?;
        for (key, val) in diff_maps {
            match val {
                Diff::None => {}
                Diff::Delete => {}
                Diff::Patch(ref v) => { 
                    map.serialize_entry(key, v)? 
                },
                Diff::Replace(ref v) => {
                    map.serialize_entry(key, v)?;
                }
                Diff::Merge(ref v ) => {
                    map.serialize_entry(key, v)?;   
                }
            }
        }

        map.end()
    }
}

impl Serialize for Diff<Value, PatchObject> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ::serde::Serializer,
    {
        match self {
            Diff::None => serializer.serialize_unit(),
            Diff::Delete => serializer.serialize_unit(),
            Diff::Patch(ref p) => p.serialize(serializer),
            Diff::Replace(ref v) => v.serialize(serializer),
            Diff::Merge(ref v) => v.serialize(serializer),
        }
    }
}

#[cfg(test)]
mod test {

    use serde_json::json;

    use crate::Changes;

    #[test]
    fn test_patch_to_simple() {
        let old_spec = json!({
            "replicas": 2,
            "apple": 5
        });
        let new_spec = json!({
            "replicas": 3,
            "apple": 5
        });

        let diff = old_spec.diff(&new_spec).expect("diff");
        assert!(diff.is_patch());

        let expected = json!({
            "replicas": 3
        });
        let json_diff = serde_json::to_value(diff).unwrap();
        assert_eq!(json_diff, expected);
    }

    #[test]
    fn test_patch_to_hierarchy() {
        let old_spec = json!({
            "spec": {
                 "replicas": 2,
                "apple": 5
            }
        });
        let new_spec = json!({
            "spec": {
                 "replicas": 3,
                "apple": 5
            }
        });

        let diff = old_spec.diff(&new_spec).expect("diff");
        assert!(diff.is_patch());
        println!("final diff: {:#?}", diff);
        let expected = json!({
            "spec": {
            "replicas": 3
        }});
        let json_diff = serde_json::to_value(diff).unwrap();
        assert_eq!(json_diff, expected);
    }

}

use crate::Crd;
use serde::Serialize;

/// related to query parameters and uri
///
///
///
/// generate prefix for given crd
/// if crd group is core then /api is used otherwise /apis + group

pub fn prefix_uri(crd: &Crd, host: &str, namespace: &str, options: Option<&ListOptions>) -> String {
    let version = crd.version;
    let plural = crd.names.plural;
    let group = crd.group.as_ref();
    let api_prefix = match group {
        "core" => "api".to_owned(),
        _ => format!("apis/{}", group),
    };

    let query = if let Some(opt) = options {
        let mut query = "?".to_owned();
        let qs = serde_qs::to_string(opt).unwrap();
        query.push_str(&qs);
        query
    } else {
        "".to_owned()
    };

    format!(
        "{}/{}/{}/namespaces/{}/{}{}",
        host, api_prefix, version, namespace, plural, query
    )
}

/// goes as query parameter
#[derive(Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListOptions {
    pub pretty: Option<bool>,
    #[serde(rename = "continue")]
    pub continu: Option<String>,
    pub field_selector: Option<String>,
    pub include_uninitialized: Option<bool>,
    pub label_selector: Option<String>,
    pub limit: Option<u32>,
    pub resource_version: Option<String>,
    pub timeout_seconds: Option<u32>,
    pub watch: Option<bool>,
}

#[derive(Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct DeleteOptions {
    pub api_version: Option<String>,
    pub grace_period_seconds: Option<u64>,
    pub kind: Option<String>,
    pub orphan_dependents: Option<String>,
    pub preconditions: Vec<Precondition>,
    pub propagation_policy: Option<String>,
}

#[derive(Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Precondition {
    pub uid: String,
}

#[cfg(test)]
mod test {

    use super::prefix_uri;
    use super::ListOptions;
    use crate::metadata::DEFAULT_NS;
    use crate::Crd;
    use crate::CrdNames;

    const G1: Crd = Crd {
        group: "test.com",
        version: "v1",
        names: CrdNames {
            kind: "Item",
            plural: "items",
            singular: "item",
        },
    };

    const C1: Crd = Crd {
        group: "core",
        version: "v1",
        names: CrdNames {
            kind: "Item",
            plural: "items",
            singular: "item",
        },
    };

    #[test]
    fn test_api_prefix_group() {
        let uri = prefix_uri(&G1, "https://localhost", DEFAULT_NS, None);
        assert_eq!(
            uri,
            "https://localhost/apis/test.com/v1/namespaces/default/items"
        );
    }

    #[test]
    fn test_api_prefix_core() {
        let uri = prefix_uri(&C1, "https://localhost", DEFAULT_NS, None);
        assert_eq!(uri, "https://localhost/api/v1/namespaces/default/items");
    }

    #[test]
    fn test_api_prefix_watch() {
        let opt = ListOptions {
            watch: Some(true),
            ..Default::default()
        };
        let uri = prefix_uri(&C1, "https://localhost", DEFAULT_NS, Some(&opt));
        assert_eq!(
            uri,
            "https://localhost/api/v1/namespaces/default/items?watch=true"
        );
    }

    #[test]
    fn test_list_query() {
        let opt = ListOptions {
            pretty: Some(true),
            watch: Some(true),
            ..Default::default()
        };

        let qs = serde_qs::to_string(&opt).unwrap();
        assert_eq!(qs, "pretty=true&watch=true")
    }

}

//! Builder for `core-site.xml`.

use std::collections::BTreeMap;

use crate::controller::ValidatedClusterConfig;

const HADOOP_SECURITY_AUTHENTICATION: &str = "hadoop.security.authentication";

pub fn build(cluster_config: &ValidatedClusterConfig) -> Option<BTreeMap<String, String>> {
    if cluster_config.needs_kerberos_core_site {
        let mut data = BTreeMap::new();
        data.insert(
            HADOOP_SECURITY_AUTHENTICATION.to_string(),
            "kerberos".to_string(),
        );
        Some(data)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::build::properties::test_support::derby_cluster_config;

    #[test]
    fn omitted_when_not_required() {
        let cluster_config = derby_cluster_config();
        assert!(build(&cluster_config).is_none());
    }

    #[test]
    fn emitted_when_required() {
        let mut cluster_config = derby_cluster_config();
        cluster_config.needs_kerberos_core_site = true;
        let data = build(&cluster_config).expect("core-site present");
        assert_eq!(
            data.get("hadoop.security.authentication"),
            Some(&"kerberos".to_string())
        );
    }
}

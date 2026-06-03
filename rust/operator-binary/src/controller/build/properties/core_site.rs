//! Builder for `core-site.xml`.
//!
//! Only emitted when Kerberos is enabled without an HDFS backend (resolved during
//! validation as [`ValidatedClusterConfig::needs_kerberos_core_site`]), in which case
//! `hadoop.security.authentication=kerberos` is required.

use std::collections::BTreeMap;

use crate::controller::ValidatedClusterConfig;

const HADOOP_SECURITY_AUTHENTICATION: &str = "hadoop.security.authentication";

/// Returns the `core-site.xml` properties, or `None` if the file should be omitted.
pub fn build(cluster_config: &ValidatedClusterConfig) -> Option<BTreeMap<String, Option<String>>> {
    if cluster_config.needs_kerberos_core_site {
        let mut data = BTreeMap::new();
        data.insert(
            HADOOP_SECURITY_AUTHENTICATION.to_string(),
            Some("kerberos".to_string()),
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
            Some(&Some("kerberos".to_string()))
        );
    }
}

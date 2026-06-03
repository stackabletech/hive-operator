//! Builder for `core-site.xml`.
//!
//! Only emitted when Kerberos is enabled and there is no HDFS backend (i.e. S3),
//! in which case `hadoop.security.authentication=kerberos` is required.

use std::collections::BTreeMap;

use crate::crd::v1alpha1;

const HADOOP_SECURITY_AUTHENTICATION: &str = "hadoop.security.authentication";

/// Returns the `core-site.xml` properties, or `None` if the file should be omitted.
pub fn build(hive: &v1alpha1::HiveCluster) -> Option<BTreeMap<String, Option<String>>> {
    if hive.has_kerberos_enabled() && hive.spec.cluster_config.hdfs.is_none() {
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

    fn hive_cluster(yaml: &str) -> v1alpha1::HiveCluster {
        stackable_operator::utils::yaml_from_str_singleton_map(yaml)
            .expect("valid HiveCluster YAML")
    }

    const NO_KERBEROS_YAML: &str = r#"
        apiVersion: hive.stackable.tech/v1alpha1
        kind: HiveCluster
        metadata:
          name: simple-hive
          namespace: default
        spec:
          image:
            productVersion: "4.0.0"
          clusterConfig:
            metadataDatabase:
              derby: {}
          metastore:
            roleGroups:
              default:
                replicas: 1
        "#;

    const KERBEROS_S3_YAML: &str = r#"
        apiVersion: hive.stackable.tech/v1alpha1
        kind: HiveCluster
        metadata:
          name: simple-hive
          namespace: default
        spec:
          image:
            productVersion: "4.0.0"
          clusterConfig:
            authentication:
              kerberos:
                secretClass: kerberos
            metadataDatabase:
              derby: {}
          metastore:
            roleGroups:
              default:
                replicas: 1
        "#;

    const KERBEROS_HDFS_YAML: &str = r#"
        apiVersion: hive.stackable.tech/v1alpha1
        kind: HiveCluster
        metadata:
          name: simple-hive
          namespace: default
        spec:
          image:
            productVersion: "4.0.0"
          clusterConfig:
            authentication:
              kerberos:
                secretClass: kerberos
            metadataDatabase:
              derby: {}
            hdfs:
              configMap: hdfs
          metastore:
            roleGroups:
              default:
                replicas: 1
        "#;

    #[test]
    fn omitted_without_kerberos() {
        let hive = hive_cluster(NO_KERBEROS_YAML);
        assert!(build(&hive).is_none());
    }

    #[test]
    fn emitted_with_kerberos_and_no_hdfs() {
        let hive = hive_cluster(KERBEROS_S3_YAML);
        let data = build(&hive).expect("core-site present");
        assert_eq!(
            data.get("hadoop.security.authentication"),
            Some(&Some("kerberos".to_string()))
        );
    }

    #[test]
    fn omitted_with_kerberos_but_hdfs_backend() {
        let hive = hive_cluster(KERBEROS_HDFS_YAML);
        assert!(build(&hive).is_none());
    }
}

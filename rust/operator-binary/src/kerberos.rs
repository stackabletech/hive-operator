use std::collections::BTreeMap;

use indoc::formatdoc;
use stackable_operator::{kube::ResourceExt, utils::cluster_info::KubernetesClusterInfo};

use crate::crd::{HIVE_SITE_XML, HiveRole, STACKABLE_CONFIG_DIR, v1alpha1};

pub fn kerberos_config_properties(
    hive: &v1alpha1::HiveCluster,
    hive_namespace: &str,
    cluster_info: &KubernetesClusterInfo,
) -> BTreeMap<String, String> {
    if !hive.has_kerberos_enabled() {
        return BTreeMap::new();
    }

    let hive_name = hive.name_any();
    let cluster_domain = &cluster_info.cluster_domain;
    let principal_host_part =
        format!("{hive_name}.{hive_namespace}.svc.{cluster_domain}@${{env.KERBEROS_REALM}}");

    BTreeMap::from([
        // Kerberos settings
        (
            "hive.metastore.kerberos.principal".to_string(),
            format!(
                "{service_name}/{principal_host_part}",
                service_name = HiveRole::MetaStore.kerberos_service_name()
            ),
        ),
        (
            "hive.metastore.client.kerberos.principal".to_string(),
            format!(
                "{service_name}/{principal_host_part}",
                service_name = HiveRole::MetaStore.kerberos_service_name()
            ),
        ),
        (
            "hive.metastore.kerberos.keytab.file".to_string(),
            "/stackable/kerberos/keytab".to_string(),
        ),
        (
            "hive.metastore.sasl.enabled".to_string(),
            "true".to_string(),
        ),
    ])
}

pub fn kerberos_container_start_commands(hive: &v1alpha1::HiveCluster) -> String {
    if !hive.has_kerberos_enabled() {
        return String::new();
    }

    let mut args = vec![formatdoc! {"
        export KERBEROS_REALM=$(grep -oP 'default_realm = \\K.*' /stackable/kerberos/krb5.conf)
        sed -i -e 's/${{env.KERBEROS_REALM}}/'\"$KERBEROS_REALM/g\" {STACKABLE_CONFIG_DIR}/{HIVE_SITE_XML}",
    }];

    if hive.spec.cluster_config.hdfs.is_some() {
        args.extend([
            formatdoc! {"
                sed -i -e 's/${{env.KERBEROS_REALM}}/'\"$KERBEROS_REALM/g\" {STACKABLE_CONFIG_DIR}/core-site.xml
                sed -i -e 's/${{env.KERBEROS_REALM}}/'\"$KERBEROS_REALM/g\" {STACKABLE_CONFIG_DIR}/hdfs-site.xml",
            }
        ]);
    }

    args.join("\n")
}

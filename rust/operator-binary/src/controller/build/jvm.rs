use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::memory::{BinaryMultiple, MemoryQuantity};

use super::{kerberos::STACKABLE_KERBEROS_DIR, properties::ConfigFileName};
use crate::{
    controller::HiveRoleGroupConfig,
    crd::{
        METRICS_PORT, MetaStoreConfig, STACKABLE_CONFIG_DIR, STACKABLE_TRUST_STORE,
        STACKABLE_TRUST_STORE_PASSWORD, v1alpha1::HiveCluster,
    },
};

const JAVA_HEAP_FACTOR: f32 = 0.8;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("invalid memory resource configuration - missing default or value in crd?"))]
    MissingMemoryResourceConfig,

    #[snafu(display("invalid memory config"))]
    InvalidMemoryConfig {
        source: stackable_operator::memory::Error,
    },
}

/// All JVM arguments.
fn construct_jvm_args(hive: &HiveCluster, rg: &HiveRoleGroupConfig) -> Vec<String> {
    let security_properties = ConfigFileName::Security;
    let mut jvm_args = vec![
        format!("-Djava.security.properties={STACKABLE_CONFIG_DIR}/{security_properties}"),
        format!(
            "-javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar={METRICS_PORT}:/stackable/jmx/jmx_hive_config.yaml"
        ),
        format!("-Djavax.net.ssl.trustStore={STACKABLE_TRUST_STORE}"),
        format!("-Djavax.net.ssl.trustStorePassword={STACKABLE_TRUST_STORE_PASSWORD}"),
        format!("-Djavax.net.ssl.trustStoreType=pkcs12"),
    ];

    if hive.has_kerberos_enabled() {
        jvm_args.push(format!(
            "-Djava.security.krb5.conf={STACKABLE_KERBEROS_DIR}/krb5.conf"
        ));
    }

    // Apply the already-merged (role + role group) JVM argument overrides on top of the
    // operator-generated base arguments.
    rg.jvm_argument_overrides.apply_to(jvm_args)
}

/// Arguments that go into `HADOOP_OPTS`, so *not* the heap settings (which you can get using
/// [`construct_hadoop_heapsize_env`]).
pub fn construct_non_heap_jvm_args(hive: &HiveCluster, rg: &HiveRoleGroupConfig) -> String {
    let mut jvm_args = construct_jvm_args(hive, rg);
    jvm_args.retain(|arg| !is_heap_jvm_argument(arg));

    jvm_args.join(" ")
}

/// This will be put into `HADOOP_HEAPSIZE`, which is just the heap size in megabytes (*without* the `m`
/// unit prepended).
pub fn construct_hadoop_heapsize_env(merged_config: &MetaStoreConfig) -> Result<String, Error> {
    let heap_size_in_mb = (MemoryQuantity::try_from(
        merged_config
            .resources
            .memory
            .limit
            .as_ref()
            .context(MissingMemoryResourceConfigSnafu)?,
    )
    .context(InvalidMemoryConfigSnafu)?
        * JAVA_HEAP_FACTOR)
        .scale_to(BinaryMultiple::Mebi);

    Ok((heap_size_in_mb.value.floor() as u32).to_string())
}

fn is_heap_jvm_argument(jvm_argument: &str) -> bool {
    let lowercase = jvm_argument.to_lowercase();

    lowercase.starts_with("-xms") || lowercase.starts_with("-xmx")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        controller::test_support::{minimal_hive, validated_cluster},
        crd::HiveRole,
    };

    fn metastore_default(hive: &crate::crd::v1alpha1::HiveCluster) -> HiveRoleGroupConfig {
        let validated = validated_cluster(hive);
        validated
            .role_group_configs
            .get(&HiveRole::MetaStore)
            .and_then(|groups| groups.get(&"default".parse().expect("valid role group name")))
            .expect("metastore default role group should exist")
            .clone()
    }

    #[test]
    fn test_construct_jvm_arguments_defaults() {
        let input = r#"
        apiVersion: hive.stackable.tech/v1alpha1
        kind: HiveCluster
        metadata:
          name: simple-hive
          namespace: default
          uid: 12345678-1234-1234-1234-123456789012
        spec:
          image:
            productVersion: 4.2.0
          clusterConfig:
            metadataDatabase:
              derby: {}
          metastore:
            roleGroups:
              default:
                replicas: 1
        "#;
        let hive = minimal_hive(input);
        let rg = metastore_default(&hive);
        let non_heap_jvm_args = construct_non_heap_jvm_args(&hive, &rg);
        let hadoop_heapsize_env = construct_hadoop_heapsize_env(&rg.config).unwrap();

        assert_eq!(
            non_heap_jvm_args,
            format!(
                "-Djava.security.properties={STACKABLE_CONFIG_DIR}/security.properties \
            -javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar={METRICS_PORT}:/stackable/jmx/jmx_hive_config.yaml \
            -Djavax.net.ssl.trustStore={STACKABLE_TRUST_STORE} \
            -Djavax.net.ssl.trustStorePassword={STACKABLE_TRUST_STORE_PASSWORD} \
            -Djavax.net.ssl.trustStoreType=pkcs12"
            )
        );
        assert_eq!(hadoop_heapsize_env, "614");
    }

    #[test]
    fn test_construct_jvm_argument_overrides() {
        let input = r#"
        apiVersion: hive.stackable.tech/v1alpha1
        kind: HiveCluster
        metadata:
          name: simple-hive
          namespace: default
          uid: 12345678-1234-1234-1234-123456789012
        spec:
          image:
            productVersion: 4.2.0
          clusterConfig:
            metadataDatabase:
              derby: {}
          metastore:
            config:
              resources:
                memory:
                  limit: 42Gi
            jvmArgumentOverrides:
              add:
                - -Dhttps.proxyHost=proxy.my.corp
                - -Dhttps.proxyPort=8080
                - -Djava.net.preferIPv4Stack=true
            roleGroups:
              default:
                replicas: 1
                jvmArgumentOverrides:
                  # We need more memory!
                  removeRegex:
                    - -Xmx.*
                    - -Dhttps.proxyPort=.*
                  add:
                    - -Xmx40000m
                    - -Dhttps.proxyPort=1234
        "#;
        let hive = minimal_hive(input);
        let rg = metastore_default(&hive);
        let non_heap_jvm_args = construct_non_heap_jvm_args(&hive, &rg);
        let hadoop_heapsize_env = construct_hadoop_heapsize_env(&rg.config).unwrap();

        assert_eq!(
            non_heap_jvm_args,
            format!(
                "-Djava.security.properties={STACKABLE_CONFIG_DIR}/security.properties \
            -javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar={METRICS_PORT}:/stackable/jmx/jmx_hive_config.yaml \
            -Djavax.net.ssl.trustStore={STACKABLE_TRUST_STORE} \
            -Djavax.net.ssl.trustStorePassword={STACKABLE_TRUST_STORE_PASSWORD} \
            -Djavax.net.ssl.trustStoreType=pkcs12 \
            -Dhttps.proxyHost=proxy.my.corp \
            -Djava.net.preferIPv4Stack=true \
            -Dhttps.proxyPort=1234"
            )
        );
        assert_eq!(hadoop_heapsize_env, "34406");
    }
}

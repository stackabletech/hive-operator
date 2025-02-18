use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    memory::{BinaryMultiple, MemoryQuantity},
    role_utils::{self, GenericRoleConfig, JavaCommonConfig, JvmArgumentOverrides, Role},
};

use crate::crd::{
    v1alpha1::HiveCluster, MetaStoreConfig, MetaStoreConfigFragment, JVM_SECURITY_PROPERTIES_FILE,
    METRICS_PORT, STACKABLE_CONFIG_DIR, STACKABLE_TRUST_STORE, STACKABLE_TRUST_STORE_PASSWORD,
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

    #[snafu(display("failed to merge jvm argument overrides"))]
    MergeJvmArgumentOverrides { source: role_utils::Error },
}

/// All JVM arguments.
fn construct_jvm_args(
    hive: &HiveCluster,
    merged_config: &MetaStoreConfig,
    role: &Role<MetaStoreConfigFragment, GenericRoleConfig, JavaCommonConfig>,
    role_group: &str,
) -> Result<Vec<String>, Error> {
    let heap_size = MemoryQuantity::try_from(
        merged_config
            .resources
            .memory
            .limit
            .as_ref()
            .context(MissingMemoryResourceConfigSnafu)?,
    )
    .context(InvalidMemoryConfigSnafu)?
    .scale_to(BinaryMultiple::Mebi)
        * JAVA_HEAP_FACTOR;
    let java_heap = heap_size
        .format_for_java()
        .context(InvalidMemoryConfigSnafu)?;

    let mut jvm_args = vec![
        // Heap settings
        format!("-Xmx{java_heap}"),
        format!("-Xms{java_heap}"),
        format!("-Djava.security.properties={STACKABLE_CONFIG_DIR}/{JVM_SECURITY_PROPERTIES_FILE}"),
        format!("-javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar={METRICS_PORT}:/stackable/jmx/jmx_hive_config.yaml"),
        format!("-Djavax.net.ssl.trustStore={STACKABLE_TRUST_STORE}"),
        format!("-Djavax.net.ssl.trustStorePassword={STACKABLE_TRUST_STORE_PASSWORD}"),
        format!("-Djavax.net.ssl.trustStoreType=pkcs12"),
    ];

    if hive.has_kerberos_enabled() {
        jvm_args.push("-Djava.security.krb5.conf=/stackable/kerberos/krb5.conf".to_owned());
    }

    let operator_generated = JvmArgumentOverrides::new_with_only_additions(jvm_args);
    let merged = role
        .get_merged_jvm_argument_overrides(role_group, &operator_generated)
        .context(MergeJvmArgumentOverridesSnafu)?;
    Ok(merged
        .effective_jvm_config_after_merging()
        // Sorry for the clone, that's how operator-rs is currently modelled :P
        .clone())
}

/// Arguments that go into `HADOOP_OPTS`, so *not* the heap settings (which you can get using
/// [`construct_heap_jvm_args`]).
pub fn construct_non_heap_jvm_args(
    hive: &HiveCluster,
    merged_config: &MetaStoreConfig,
    role: &Role<MetaStoreConfigFragment, GenericRoleConfig, JavaCommonConfig>,
    role_group: &str,
) -> Result<String, Error> {
    let mut jvm_args = construct_jvm_args(hive, merged_config, role, role_group)?;
    jvm_args.retain(|arg| !is_heap_jvm_argument(arg));

    Ok(jvm_args.join(" "))
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
    use crate::crd::HiveRole;

    #[test]
    fn test_construct_jvm_arguments_defaults() {
        let input = r#"
        apiVersion: hive.stackable.tech/v1alpha1
        kind: HiveCluster
        metadata:
          name: simple-hive
        spec:
          image:
            productVersion: 4.0.0
          clusterConfig:
            database:
              connString: jdbc:derby:;databaseName=/tmp/hive;create=true
              dbType: derby
              credentialsSecret: mySecret
          metastore:
            roleGroups:
              default:
                replicas: 1
        "#;
        let (hive, merged_config, role, rolegroup) = construct_boilerplate(input);
        let non_heap_jvm_args =
            construct_non_heap_jvm_args(&hive, &merged_config, &role, &rolegroup).unwrap();
        let hadoop_heapsize_env = construct_hadoop_heapsize_env(&merged_config).unwrap();

        assert_eq!(
            non_heap_jvm_args,
            "-Djava.security.properties=/stackable/config/security.properties \
            -javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar=9084:/stackable/jmx/jmx_hive_config.yaml \
            -Djavax.net.ssl.trustStore=/stackable/truststore.p12 \
            -Djavax.net.ssl.trustStorePassword=changeit \
            -Djavax.net.ssl.trustStoreType=pkcs12"
        );
        assert_eq!(hadoop_heapsize_env, "409");
    }

    #[test]
    fn test_construct_jvm_argument_overrides() {
        let input = r#"
        apiVersion: hive.stackable.tech/v1alpha1
        kind: HiveCluster
        metadata:
          name: simple-hive
        spec:
          image:
            productVersion: 4.0.0
          clusterConfig:
            database:
              connString: jdbc:derby:;databaseName=/tmp/hive;create=true
              dbType: derby
              credentialsSecret: mySecret
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
        let (hive, merged_config, role, rolegroup) = construct_boilerplate(input);
        let non_heap_jvm_args =
            construct_non_heap_jvm_args(&hive, &merged_config, &role, &rolegroup).unwrap();
        let hadoop_heapsize_env = construct_hadoop_heapsize_env(&merged_config).unwrap();

        assert_eq!(
            non_heap_jvm_args,
            "-Djava.security.properties=/stackable/config/security.properties \
            -javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar=9084:/stackable/jmx/jmx_hive_config.yaml \
            -Djavax.net.ssl.trustStore=/stackable/truststore.p12 \
            -Djavax.net.ssl.trustStorePassword=changeit \
            -Djavax.net.ssl.trustStoreType=pkcs12 \
            -Dhttps.proxyHost=proxy.my.corp \
            -Djava.net.preferIPv4Stack=true \
            -Dhttps.proxyPort=1234"
        );
        assert_eq!(hadoop_heapsize_env, "34406");
    }

    fn construct_boilerplate(
        hive_cluster: &str,
    ) -> (
        HiveCluster,
        MetaStoreConfig,
        Role<MetaStoreConfigFragment, GenericRoleConfig, JavaCommonConfig>,
        String,
    ) {
        let hive: HiveCluster = serde_yaml::from_str(hive_cluster).expect("illegal test input");

        let hive_role = HiveRole::MetaStore;
        let rolegroup_ref = hive.metastore_rolegroup_ref("default");
        let merged_config = hive.merged_config(&hive_role, &rolegroup_ref).unwrap();
        let role = hive.spec.metastore.clone().unwrap();

        (hive, merged_config, role, "default".to_owned())
    }
}

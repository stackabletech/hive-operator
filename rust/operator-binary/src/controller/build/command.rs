use stackable_operator::crd::s3;

use super::{
    opa::build_opa_tls_ca_cert_mount_path, properties::ConfigFileName,
    resource::statefulset::HDFS_CONFIG_MOUNT_DIR,
};
use crate::{
    controller::{ValidatedCluster, dereference::ResolvedOpaConfig},
    crd::{
        STACKABLE_CONFIG_DIR, STACKABLE_CONFIG_MOUNT_DIR, STACKABLE_LOG_CONFIG_MOUNT_DIR,
        STACKABLE_TRUST_STORE, STACKABLE_TRUST_STORE_PASSWORD,
    },
};

pub fn build_container_command_args(
    cluster: &ValidatedCluster,
    start_command: String,
    s3_connection_spec: Option<&s3::v1alpha1::ConnectionSpec>,
    hive_opa_config: Option<&ResolvedOpaConfig>,
) -> Vec<String> {
    let log4j2_properties = ConfigFileName::Log4j2;
    let core_site = ConfigFileName::CoreSite;
    let hive_site = ConfigFileName::HiveSite;
    let mut args = vec![
        // copy config files to a writeable empty folder in order to set s3 access and secret keys
        format!("echo copying {STACKABLE_CONFIG_MOUNT_DIR} to {STACKABLE_CONFIG_DIR}"),
        format!("cp -RL {STACKABLE_CONFIG_MOUNT_DIR}/* {STACKABLE_CONFIG_DIR}"),
        // Copy log4j2 properties
        format!(
            "echo copying {STACKABLE_LOG_CONFIG_MOUNT_DIR}/{log4j2_properties} to {STACKABLE_CONFIG_DIR}/{log4j2_properties}"
        ),
        format!(
            "cp -RL {STACKABLE_LOG_CONFIG_MOUNT_DIR}/{log4j2_properties} {STACKABLE_CONFIG_DIR}/{log4j2_properties}"
        ),
        // Template config files
        format!(
            "if test -f {STACKABLE_CONFIG_DIR}/{core_site}; then config-utils template {STACKABLE_CONFIG_DIR}/{core_site}; fi"
        ),
        format!(
            "if test -f {STACKABLE_CONFIG_DIR}/{hive_site}; then config-utils template {STACKABLE_CONFIG_DIR}/{hive_site}; fi"
        ),
        // Copy system truststore to stackable truststore
        format!(
            "cert-tools generate-pkcs12-truststore --pem /etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem --out {STACKABLE_TRUST_STORE} --out-password {STACKABLE_TRUST_STORE_PASSWORD}"
        ),
    ];

    if cluster.cluster_config.hdfs.is_some() {
        args.extend([
            format!("echo copying {HDFS_CONFIG_MOUNT_DIR} to {STACKABLE_CONFIG_DIR}"),
            format!("cp -RL {HDFS_CONFIG_MOUNT_DIR}/* {STACKABLE_CONFIG_DIR}"),
        ]);
    }

    if let Some(s3) = s3_connection_spec
        && let Some(ca_cert_file) = s3.tls.tls_ca_cert_mount_path()
    {
        args.push(format!(
                "cert-tools generate-pkcs12-truststore --pkcs12 {STACKABLE_TRUST_STORE}:{STACKABLE_TRUST_STORE_PASSWORD} --pem {ca_cert_file} --out {STACKABLE_TRUST_STORE} --out-password {STACKABLE_TRUST_STORE_PASSWORD}"
            ));
    }

    if let Some(opa) = hive_opa_config
        && let Some(ca_cert_dir) = build_opa_tls_ca_cert_mount_path(opa)
    {
        args.push(format!(
                "cert-tools generate-pkcs12-truststore --pkcs12 {STACKABLE_TRUST_STORE}:{STACKABLE_TRUST_STORE_PASSWORD} --pem {ca_cert_dir}/ca.crt --out {STACKABLE_TRUST_STORE} --out-password {STACKABLE_TRUST_STORE_PASSWORD}"
            ));
    }

    // metastore start command
    args.push(start_command);

    vec![args.join("\n")]
}

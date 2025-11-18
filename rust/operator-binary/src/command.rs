use stackable_operator::crd::s3;

use crate::{
    config::opa::HiveOpaConfig,
    crd::{
        DB_PASSWORD_ENV, DB_PASSWORD_PLACEHOLDER, DB_USERNAME_ENV, DB_USERNAME_PLACEHOLDER,
        HIVE_METASTORE_LOG4J2_PROPERTIES, HIVE_SITE_XML, STACKABLE_CONFIG_DIR,
        STACKABLE_CONFIG_MOUNT_DIR, STACKABLE_LOG_CONFIG_MOUNT_DIR, STACKABLE_TRUST_STORE,
        STACKABLE_TRUST_STORE_PASSWORD, v1alpha1,
    },
};

pub fn build_container_command_args(
    hive: &v1alpha1::HiveCluster,
    start_command: String,
    s3_connection_spec: Option<&s3::v1alpha1::ConnectionSpec>,
    hive_opa_config: Option<&HiveOpaConfig>,
) -> Vec<String> {
    let mut args = vec![
        // copy config files to a writeable empty folder in order to set s3 access and secret keys
        format!("echo copying {STACKABLE_CONFIG_MOUNT_DIR} to {STACKABLE_CONFIG_DIR}"),
        format!("cp -RL {STACKABLE_CONFIG_MOUNT_DIR}/* {STACKABLE_CONFIG_DIR}"),
        // Copy log4j2 properties
        format!(
            "echo copying {STACKABLE_LOG_CONFIG_MOUNT_DIR}/{HIVE_METASTORE_LOG4J2_PROPERTIES} to {STACKABLE_CONFIG_DIR}/{HIVE_METASTORE_LOG4J2_PROPERTIES}"
        ),
        format!(
            "cp -RL {STACKABLE_LOG_CONFIG_MOUNT_DIR}/{HIVE_METASTORE_LOG4J2_PROPERTIES} {STACKABLE_CONFIG_DIR}/{HIVE_METASTORE_LOG4J2_PROPERTIES}"
        ),
        // Template config files
        format!(
            "if test -f {STACKABLE_CONFIG_DIR}/core-site.xml; then config-utils template {STACKABLE_CONFIG_DIR}/core-site.xml; fi"
        ),
        format!(
            "if test -f {STACKABLE_CONFIG_DIR}/hive-site.xml; then config-utils template {STACKABLE_CONFIG_DIR}/hive-site.xml; fi"
        ),
        // Copy system truststore to stackable truststore
        format!(
            "cert-tools generate-pkcs12-truststore --pem /etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem --out {STACKABLE_TRUST_STORE} --out-password {STACKABLE_TRUST_STORE_PASSWORD}"
        ),
    ];

    if hive.spec.cluster_config.hdfs.is_some() {
        args.extend([
            format!("echo copying /stackable/mount/hdfs-config to {STACKABLE_CONFIG_DIR}"),
            format!("cp -RL /stackable/mount/hdfs-config/* {STACKABLE_CONFIG_DIR}"),
        ]);
    }

    if let Some(s3) = s3_connection_spec {
        if let Some(ca_cert_file) = s3.tls.tls_ca_cert_mount_path() {
            args.push(format!(
                "cert-tools generate-pkcs12-truststore --pkcs12 {STACKABLE_TRUST_STORE}:{STACKABLE_TRUST_STORE_PASSWORD} --pem {ca_cert_file} --out {STACKABLE_TRUST_STORE} --out-password {STACKABLE_TRUST_STORE_PASSWORD}"
            ));
        }
    }

    if let Some(opa) = hive_opa_config {
        if let Some(ca_cert_dir) = opa.tls_ca_cert_mount_path() {
            args.push(format!(
                "cert-tools generate-pkcs12-truststore --pkcs12 {STACKABLE_TRUST_STORE}:{STACKABLE_TRUST_STORE_PASSWORD} --pem {ca_cert_dir}/ca.crt --out {STACKABLE_TRUST_STORE} --out-password {STACKABLE_TRUST_STORE_PASSWORD}"
            ));
        }
    }

    // db credentials
    args.extend([
        format!("echo replacing {DB_USERNAME_PLACEHOLDER} and {DB_PASSWORD_PLACEHOLDER} with secret values."),
        format!("sed -i \"s|{DB_USERNAME_PLACEHOLDER}|${DB_USERNAME_ENV}|g\" {STACKABLE_CONFIG_DIR}/{HIVE_SITE_XML}"),
        format!("sed -i \"s|{DB_PASSWORD_PLACEHOLDER}|${DB_PASSWORD_ENV}|g\" {STACKABLE_CONFIG_DIR}/{HIVE_SITE_XML}"),
    ]);

    // metastore start command
    args.push(start_command);

    vec![args.join("\n")]
}

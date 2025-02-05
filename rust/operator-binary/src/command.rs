use stackable_operator::commons::s3::S3ConnectionSpec;

use crate::crd::{
    HiveCluster, DB_PASSWORD_ENV, DB_PASSWORD_PLACEHOLDER, DB_USERNAME_ENV,
    DB_USERNAME_PLACEHOLDER, HIVE_METASTORE_LOG4J2_PROPERTIES, HIVE_SITE_XML, STACKABLE_CONFIG_DIR,
    STACKABLE_CONFIG_MOUNT_DIR, STACKABLE_LOG_CONFIG_MOUNT_DIR, STACKABLE_TRUST_STORE,
    STACKABLE_TRUST_STORE_PASSWORD, SYSTEM_TRUST_STORE, SYSTEM_TRUST_STORE_PASSWORD,
};

pub fn build_container_command_args(
    hive: &HiveCluster,
    start_command: String,
    s3_connection_spec: Option<&S3ConnectionSpec>,
) -> Vec<String> {
    let mut args = vec![
        // copy config files to a writeable empty folder in order to set s3 access and secret keys
        format!("echo copying {STACKABLE_CONFIG_MOUNT_DIR} to {STACKABLE_CONFIG_DIR}"),
        format!("cp -RL {STACKABLE_CONFIG_MOUNT_DIR}/* {STACKABLE_CONFIG_DIR}"),

        // Copy log4j2 properties
        format!("echo copying {STACKABLE_LOG_CONFIG_MOUNT_DIR}/{HIVE_METASTORE_LOG4J2_PROPERTIES} to {STACKABLE_CONFIG_DIR}/{HIVE_METASTORE_LOG4J2_PROPERTIES}"),
        format!("cp -RL {STACKABLE_LOG_CONFIG_MOUNT_DIR}/{HIVE_METASTORE_LOG4J2_PROPERTIES} {STACKABLE_CONFIG_DIR}/{HIVE_METASTORE_LOG4J2_PROPERTIES}"),

        // Template config files
        format!("if test -f {STACKABLE_CONFIG_DIR}/core-site.xml; then config-utils template {STACKABLE_CONFIG_DIR}/core-site.xml; fi"),
        format!("if test -f {STACKABLE_CONFIG_DIR}/hive-site.xml; then config-utils template {STACKABLE_CONFIG_DIR}/hive-site.xml; fi"),

        // Copy system truststore to stackable truststore
        format!("keytool -importkeystore -srckeystore {SYSTEM_TRUST_STORE} -srcstoretype jks -srcstorepass {SYSTEM_TRUST_STORE_PASSWORD} -destkeystore {STACKABLE_TRUST_STORE} -deststoretype pkcs12 -deststorepass {STACKABLE_TRUST_STORE_PASSWORD} -noprompt")
    ];

    if hive.spec.cluster_config.hdfs.is_some() {
        args.extend([
            format!("echo copying /stackable/mount/hdfs-config to {STACKABLE_CONFIG_DIR}"),
            format!("cp -RL /stackable/mount/hdfs-config/* {STACKABLE_CONFIG_DIR}"),
        ]);
    }

    if let Some(s3) = s3_connection_spec {
        if let Some(ca_cert) = s3.tls.tls_ca_cert_mount_path() {
            // The alias can not clash, as we only support a single S3Connection
            args.push(format!("keytool -importcert -file {ca_cert} -alias stackable-s3-ca-cert -keystore {STACKABLE_TRUST_STORE} -storepass {STACKABLE_TRUST_STORE_PASSWORD} -noprompt"));
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

use stackable_hive_crd::{
    HIVE_SITE_XML, LOG4J_PROPERTIES, STACKABLE_CONFIG_DIR, STACKABLE_CONFIG_MOUNT_DIR,
    STACKABLE_LOG_MOUNT_DIR, STACKABLE_TRUST_STORE, STACKABLE_TRUST_STORE_PASSWORD,
    SYSTEM_TRUST_STORE, SYSTEM_TRUST_STORE_PASSWORD,
};
use stackable_operator::commons::{
    s3::S3ConnectionSpec,
    tls::{CaCert, Tls, TlsServerVerification, TlsVerification},
};

pub const S3_SECRET_DIR: &str = "/stackable/secrets";
pub const S3_ACCESS_KEY: &str = "accessKey";
pub const S3_SECRET_KEY: &str = "secretKey";
pub const ACCESS_KEY_PLACEHOLDER: &str = "xxx_access_key_xxx";
pub const SECRET_KEY_PLACEHOLDER: &str = "xxx_secret_key_xxx";

pub fn build_container_command_args(
    start_command: String,
    s3_connection_spec: Option<&S3ConnectionSpec>,
) -> Vec<String> {
    let mut args = vec![
        // copy config files to a writeable empty folder in order to set s3 access and secret keys
        format!("echo copying {STACKABLE_CONFIG_MOUNT_DIR} to {STACKABLE_CONFIG_DIR}"),
        format!("cp -RL {STACKABLE_CONFIG_MOUNT_DIR}/* {STACKABLE_CONFIG_DIR}"),

        // TODO: test
        format!("echo copying {STACKABLE_LOG_MOUNT_DIR}/{LOG4J_PROPERTIES} to {STACKABLE_CONFIG_DIR}/hive-{LOG4J_PROPERTIES}"),
        format!("cp -RL {STACKABLE_LOG_MOUNT_DIR}/{LOG4J_PROPERTIES} {STACKABLE_CONFIG_DIR}/hive-log4j2.properties"),

        // Copy system truststore to stackable truststore
        format!("keytool -importkeystore -srckeystore {SYSTEM_TRUST_STORE} -srcstoretype jks -srcstorepass {SYSTEM_TRUST_STORE_PASSWORD} -destkeystore {STACKABLE_TRUST_STORE} -deststoretype pkcs12 -deststorepass {STACKABLE_TRUST_STORE_PASSWORD} -noprompt")
    ];

    if let Some(s3) = s3_connection_spec {
        if s3.credentials.is_some() {
            args.extend([
                format!("echo replacing {ACCESS_KEY_PLACEHOLDER} and {SECRET_KEY_PLACEHOLDER} with secret values."),
                format!("sed -i \"s|{ACCESS_KEY_PLACEHOLDER}|$(cat {S3_SECRET_DIR}/{S3_ACCESS_KEY})|g\" {STACKABLE_CONFIG_DIR}/{HIVE_SITE_XML}"),
                format!("sed -i \"s|{SECRET_KEY_PLACEHOLDER}|$(cat {S3_SECRET_DIR}/{S3_SECRET_KEY})|g\" {STACKABLE_CONFIG_DIR}/{HIVE_SITE_XML}"),
            ]);
        }

        if let Some(Tls {
            verification:
                TlsVerification::Server(TlsServerVerification {
                    ca_cert: CaCert::SecretClass(secret_class),
                }),
        }) = &s3.tls
        {
            args.push(format!("keytool -importcert -file /stackable/certificates/{secret_class}-tls-certificate/ca.crt -alias stackable-{secret_class} -keystore {STACKABLE_TRUST_STORE} -storepass {STACKABLE_TRUST_STORE_PASSWORD} -noprompt"));
        }
    }
    // metastore start command
    args.push(start_command);

    vec![args.join(" && ")]
}

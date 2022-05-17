use stackable_hive_crd::{HIVE_SITE_XML, STACKABLE_CONFIG_DIR, STACKABLE_RW_CONFIG_DIR};
use stackable_operator::commons::s3::S3ConnectionSpec;

pub const S3_SECRET_DIR: &str = "/stackable/secrets";
pub const S3_ACCESS_KEY: &str = "accessKey";
pub const S3_SECRET_KEY: &str = "secretKey";
pub const ACCESS_KEY_PLACEHOLDER: &str = "xxx_access_key_xxx";
pub const SECRET_KEY_PLACEHOLDER: &str = "xxx_secret_key_xxx";

pub fn build_container_command_args(
    start_command: String,
    s3_connection_spec: Option<&S3ConnectionSpec>,
) -> Vec<String> {
    let mut args = vec![];

    args.extend(vec![
        // copy config files to a writeable empty folder in order to set
        // s3 access and secret keys
        format!("echo copying {STACKABLE_CONFIG_DIR} to {STACKABLE_RW_CONFIG_DIR}"),
        format!("cp -RL {STACKABLE_CONFIG_DIR}/* {STACKABLE_RW_CONFIG_DIR}"),
    ]);

    if let Some(s3) = s3_connection_spec {
        if s3.credentials.is_some() {
            args.extend([
                format!("echo replacing {ACCESS_KEY_PLACEHOLDER} and {SECRET_KEY_PLACEHOLDER} with secret values."),
                format!("sed -i \"s|{ACCESS_KEY_PLACEHOLDER}|$(cat {S3_SECRET_DIR}/{S3_ACCESS_KEY})|g\" {STACKABLE_RW_CONFIG_DIR}/{HIVE_SITE_XML}"),
                format!("sed -i \"s|{SECRET_KEY_PLACEHOLDER}|$(cat {S3_SECRET_DIR}/{S3_SECRET_KEY})|g\" {STACKABLE_RW_CONFIG_DIR}/{HIVE_SITE_XML}"),
            ]);
        }
    }
    // metastore start command
    args.push(start_command);

    vec![args.join(" && ")]
}

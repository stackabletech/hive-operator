use indoc::formatdoc;
use snafu::{ResultExt, Snafu};
use stackable_hive_crd::{
    HiveCluster, HiveRole, HIVE_SITE_XML, STACKABLE_CONFIG_DIR, TLS_STORE_DIR, TLS_STORE_PASSWORD,
    TLS_STORE_VOLUME_NAME,
};
use stackable_operator::builder::{
    ContainerBuilder, PodBuilder, SecretFormat, SecretOperatorVolumeSourceBuilder,
    SecretOperatorVolumeSourceBuilderError, VolumeBuilder,
};
use stackable_operator::kube::ResourceExt;
use std::collections::BTreeMap;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to add Kerberos secret volume"))]
    AddKerberosSecretVolume {
        source: SecretOperatorVolumeSourceBuilderError,
    },
    #[snafu(display("failed to add TLS secret volume"))]
    AddTlsSecretVolume {
        source: SecretOperatorVolumeSourceBuilderError,
    },
}

pub fn add_kerberos_pod_config(
    hive: &HiveCluster,
    role: &HiveRole,
    cb: &mut ContainerBuilder,
    pb: &mut PodBuilder,
) -> Result<(), Error> {
    if let Some(kerberos_secret_class) = hive.kerberos_secret_class() {
        // Mount keytab
        let kerberos_secret_operator_volume =
            SecretOperatorVolumeSourceBuilder::new(kerberos_secret_class)
                .with_service_scope(hive.name_any())
                .with_kerberos_service_name(role.kerberos_service_name())
                .with_kerberos_service_name("HTTP")
                .build()
                .context(AddKerberosSecretVolumeSnafu)?;
        pb.add_volume(
            VolumeBuilder::new("kerberos")
                .ephemeral(kerberos_secret_operator_volume)
                .build(),
        );
        cb.add_volume_mount("kerberos", "/stackable/kerberos");

        // Needed env vars
        cb.add_env_var("KRB5_CONFIG", "/stackable/kerberos/krb5.conf");
    }

    if let Some(https_secret_class) = hive.https_secret_class() {
        // TLS certs
        pb.add_volume(
            VolumeBuilder::new(TLS_STORE_VOLUME_NAME)
                .ephemeral(
                    SecretOperatorVolumeSourceBuilder::new(https_secret_class)
                        .with_pod_scope()
                        .with_node_scope()
                        .with_format(SecretFormat::TlsPkcs12)
                        .with_tls_pkcs12_password(TLS_STORE_PASSWORD)
                        .build()
                        .context(AddTlsSecretVolumeSnafu)?,
                )
                .build(),
        );
        cb.add_volume_mount(TLS_STORE_VOLUME_NAME, TLS_STORE_DIR);
    }
    Ok(())
}

pub fn kerberos_config_properties(
    hive: &HiveCluster,
    hive_name: &str,
    hive_namespace: &str,
) -> BTreeMap<String, String> {
    if !hive.has_kerberos_enabled() {
        return BTreeMap::new();
    }

    let principal_host_part = principal_host_part(hive_name, hive_namespace);

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
        // Enabled https as well
        (
            "hive.metastore.sasl.enabled".to_string(),
            "true".to_string(),
        ),
    ])
}

pub fn kerberos_container_start_commands(hive: &HiveCluster) -> String {
    if !hive.has_kerberos_enabled() {
        return String::new();
    }

    formatdoc! {"
        export KERBEROS_REALM=$(grep -oP 'default_realm = \\K.*' /stackable/kerberos/krb5.conf)
        sed -i -e 's/${{env.KERBEROS_REALM}}/'\"$KERBEROS_REALM/g\" {STACKABLE_CONFIG_DIR}/{HIVE_SITE_XML}
        sed -i -e 's/${{env.KERBEROS_REALM}}/'\"$KERBEROS_REALM/g\" {STACKABLE_CONFIG_DIR}/core-site.xml
        sed -i -e 's/${{env.KERBEROS_REALM}}/'\"$KERBEROS_REALM/g\" {STACKABLE_CONFIG_DIR}/hdfs-site.xml",
    }
}
fn principal_host_part(hive_name: &str, hive_namespace: &str) -> String {
    format!("{hive_name}.{hive_namespace}.svc.cluster.local@${{env.KERBEROS_REALM}}")
}

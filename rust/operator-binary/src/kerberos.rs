use indoc::formatdoc;
use snafu::{ResultExt, Snafu};
use stackable_hive_crd::{HiveCluster, HiveRole, HIVE_SITE_XML, STACKABLE_CONFIG_DIR};
use stackable_operator::{
    builder::{
        self,
        pod::{
            container::ContainerBuilder,
            volume::{
                SecretOperatorVolumeSourceBuilder, SecretOperatorVolumeSourceBuilderError,
                VolumeBuilder,
            },
            PodBuilder,
        },
    },
    kube::ResourceExt,
    utils::cluster_info::KubernetesClusterInfo,
};
use std::collections::BTreeMap;

#[derive(Snafu, Debug)]
#[allow(clippy::enum_variant_names)] // all variants have the same prefix: `Add`
pub enum Error {
    #[snafu(display("failed to add Kerberos secret volume"))]
    AddKerberosSecretVolume {
        source: SecretOperatorVolumeSourceBuilderError,
    },

    #[snafu(display("failed to add needed volume"))]
    AddVolume { source: builder::pod::Error },

    #[snafu(display("failed to add needed volumeMount"))]
    AddVolumeMount {
        source: builder::pod::container::Error,
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
                .build()
                .context(AddKerberosSecretVolumeSnafu)?;
        pb.add_volume(
            VolumeBuilder::new("kerberos")
                .ephemeral(kerberos_secret_operator_volume)
                .build(),
        )
        .context(AddVolumeSnafu)?;
        cb.add_volume_mount("kerberos", "/stackable/kerberos")
            .context(AddVolumeMountSnafu)?;

        // Needed env vars
        cb.add_env_var("KRB5_CONFIG", "/stackable/kerberos/krb5.conf");
    }

    Ok(())
}

pub fn kerberos_config_properties(
    hive: &HiveCluster,
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

pub fn kerberos_container_start_commands(hive: &HiveCluster) -> String {
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

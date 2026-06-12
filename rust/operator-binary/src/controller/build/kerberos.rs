use std::{collections::BTreeMap, str::FromStr};

use indoc::formatdoc;
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{
        self,
        pod::{
            PodBuilder,
            container::ContainerBuilder,
            volume::{
                SecretOperatorVolumeSourceBuilder, SecretOperatorVolumeSourceBuilderError,
                VolumeBuilder,
            },
        },
    },
    commons::secret_class::SecretClassVolumeProvisionParts,
    kube::ResourceExt,
    utils::cluster_info::KubernetesClusterInfo,
    v2::types::kubernetes::VolumeName,
};

use super::properties::ConfigFileName;
use crate::crd::{HiveRole, STACKABLE_CONFIG_DIR, v1alpha1};

// Typed name for the Kerberos secret-operator volume, reusing the existing `"kerberos"` string
// value so the produced volume/mount name is unchanged.
stackable_operator::constant!(pub(crate) KERBEROS_VOLUME_NAME: VolumeName = "kerberos");

/// The directory the Kerberos secret-operator volume is mounted at. `krb5.conf` and `keytab`
/// sub-paths are derived from this.
pub(crate) const STACKABLE_KERBEROS_DIR: &str = "/stackable/kerberos";

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
    hive: &v1alpha1::HiveCluster,
    role: &HiveRole,
    cb: &mut ContainerBuilder,
    pb: &mut PodBuilder,
) -> Result<(), Error> {
    if let Some(kerberos_secret_class) = hive.kerberos_secret_class() {
        // Mount keytab
        let kerberos_secret_operator_volume = SecretOperatorVolumeSourceBuilder::new(
            kerberos_secret_class,
            // We need both public (krb5.conf) and private (keytab) parts.
            SecretClassVolumeProvisionParts::PublicPrivate,
        )
        .with_service_scope(hive.name_any())
        .with_kerberos_service_name(role.kerberos_service_name())
        .build()
        .context(AddKerberosSecretVolumeSnafu)?;
        pb.add_volume(
            VolumeBuilder::new(&*KERBEROS_VOLUME_NAME)
                .ephemeral(kerberos_secret_operator_volume)
                .build(),
        )
        .context(AddVolumeSnafu)?;
        cb.add_volume_mount(&*KERBEROS_VOLUME_NAME, STACKABLE_KERBEROS_DIR)
            .context(AddVolumeMountSnafu)?;

        // Needed env vars
        cb.add_env_var("KRB5_CONFIG", format!("{STACKABLE_KERBEROS_DIR}/krb5.conf"));
    }

    Ok(())
}

pub fn kerberos_config_properties(
    hive_name: &str,
    hive_namespace: &str,
    cluster_info: &KubernetesClusterInfo,
) -> BTreeMap<String, String> {
    let cluster_domain = &cluster_info.cluster_domain;
    let principal_host_part =
        format!("{hive_name}.{hive_namespace}.svc.{cluster_domain}@${{env.KERBEROS_REALM}}");
    let metastore_principal = format!(
        "{service_name}/{principal_host_part}",
        service_name = HiveRole::MetaStore.kerberos_service_name()
    );

    BTreeMap::from([
        // Kerberos settings
        (
            "hive.metastore.kerberos.principal".to_string(),
            metastore_principal.clone(),
        ),
        (
            "hive.metastore.client.kerberos.principal".to_string(),
            metastore_principal,
        ),
        (
            "hive.metastore.kerberos.keytab.file".to_string(),
            format!("{STACKABLE_KERBEROS_DIR}/keytab"),
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

    let hive_site_xml = ConfigFileName::HiveSite;
    let mut args = vec![formatdoc! {"
        export KERBEROS_REALM=$(grep -oP 'default_realm = \\K.*' {STACKABLE_KERBEROS_DIR}/krb5.conf)
        sed -i -e 's/${{env.KERBEROS_REALM}}/'\"$KERBEROS_REALM/g\" {STACKABLE_CONFIG_DIR}/{hive_site_xml}",
    }];

    if hive.spec.cluster_config.hdfs.is_some() {
        let core_site_xml = ConfigFileName::CoreSite;
        let hdfs_site_xml = ConfigFileName::HdfsSite;
        args.extend([
            formatdoc! {"
                sed -i -e 's/${{env.KERBEROS_REALM}}/'\"$KERBEROS_REALM/g\" {STACKABLE_CONFIG_DIR}/{core_site_xml}
                sed -i -e 's/${{env.KERBEROS_REALM}}/'\"$KERBEROS_REALM/g\" {STACKABLE_CONFIG_DIR}/{hdfs_site_xml}",
            }
        ]);
    }

    args.join("\n")
}

//! Build the per-rolegroup `ConfigMap` for the Hive metastore.

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    k8s_openapi::api::core::v1::ConfigMap,
    role_utils::RoleGroupRef,
    utils::cluster_info::KubernetesClusterInfo,
};

use crate::{
    controller::{
        ValidatedCluster,
        build::properties::{core_site, hive_site, resolved_overrides, security_properties},
        build_recommended_labels,
    },
    crd::{CORE_SITE_XML, HIVE_SITE_XML, HiveRole, JVM_SECURITY_PROPERTIES_FILE, v1alpha1},
    framework::writer::{to_hadoop_xml, to_java_properties_string},
    product_logging::extend_role_group_config_map,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("missing rolegroup {role_group} under role metastore"))]
    MissingRoleGroup { role_group: String },

    #[snafu(display("failed to build hive-site.xml"))]
    BuildHiveSite { source: hive_site::Error },

    #[snafu(display("failed to serialize {JVM_SECURITY_PROPERTIES_FILE}"))]
    WriteSecurityProperties {
        source: crate::framework::writer::PropertiesWriterError,
    },

    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to build metadata"))]
    MetadataBuild {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to add the logging configuration to the ConfigMap {cm_name}"))]
    InvalidLoggingConfig {
        source: crate::product_logging::Error,
        cm_name: String,
    },

    #[snafu(display("failed to assemble ConfigMap for {rolegroup}"))]
    Assemble {
        source: stackable_operator::builder::configmap::Error,
        rolegroup: RoleGroupRef<v1alpha1::HiveCluster>,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// The rolegroup [`ConfigMap`] configures the rolegroup based on the configuration given by the
/// administrator.
pub fn build_metastore_rolegroup_config_map(
    hive: &v1alpha1::HiveCluster,
    hive_namespace: &str,
    cluster: &ValidatedCluster,
    rolegroup: &RoleGroupRef<v1alpha1::HiveCluster>,
    cluster_info: &KubernetesClusterInfo,
) -> Result<ConfigMap> {
    let rg = cluster
        .role_group_configs
        .get(&HiveRole::MetaStore)
        .and_then(|groups| groups.get(&rolegroup.role_group))
        .with_context(|| MissingRoleGroupSnafu {
            role_group: rolegroup.role_group.clone(),
        })?;

    // hive-site.xml
    let hive_site_overrides = resolved_overrides(rg.config_overrides.hive_site_xml.clone());
    let hive_site_data = hive_site::build(
        hive,
        hive_namespace,
        &cluster.image.product_version,
        &rg.config,
        &cluster.cluster_config.metadata_database_connection_details,
        cluster.cluster_config.s3_connection_spec.as_ref(),
        cluster.cluster_config.hive_opa_config.as_ref(),
        cluster_info,
        hive_site_overrides,
    )
    .context(BuildHiveSiteSnafu)?;

    // security.properties
    let security_overrides = resolved_overrides(rg.config_overrides.security_properties.clone());
    let security_data = security_properties::build(security_overrides);

    let mut cm_builder = ConfigMapBuilder::new();
    cm_builder
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(hive)
                .name(rolegroup.object_name())
                .ownerreference_from_resource(hive, None, Some(true))
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
                .with_recommended_labels(&build_recommended_labels(
                    hive,
                    &cluster.image.app_version_label_value,
                    &rolegroup.role,
                    &rolegroup.role_group,
                ))
                .context(MetadataBuildSnafu)?
                .build(),
        )
        .add_data(HIVE_SITE_XML, to_hadoop_xml(hive_site_data.iter()))
        .add_data(
            JVM_SECURITY_PROPERTIES_FILE,
            to_java_properties_string(security_data.iter())
                .context(WriteSecurityPropertiesSnafu)?,
        );

    // core-site.xml is only required when Kerberos is enabled without an HDFS backend.
    if let Some(core_site_data) = core_site::build(hive) {
        cm_builder.add_data(CORE_SITE_XML, to_hadoop_xml(core_site_data.iter()));
    }

    extend_role_group_config_map(rolegroup, &rg.config.logging, &mut cm_builder).context(
        InvalidLoggingConfigSnafu {
            cm_name: rolegroup.object_name(),
        },
    )?;

    cm_builder.build().with_context(|_| AssembleSnafu {
        rolegroup: rolegroup.clone(),
    })
}

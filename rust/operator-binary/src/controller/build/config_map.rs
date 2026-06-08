//! Build the per-rolegroup `ConfigMap` for the Hive metastore.

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    k8s_openapi::api::core::v1::ConfigMap,
    product_logging::framework::VECTOR_CONFIG_FILE,
    role_utils::RoleGroupRef,
    v2::config_file_writer::{PropertiesWriterError, to_hadoop_xml, to_java_properties_string},
};

use crate::{
    controller::{
        HiveRoleGroupConfig, ValidatedCluster,
        build::properties::{
            ConfigFileName, core_site, hive_site, logging, resolved_overrides, security_properties,
        },
        build_recommended_labels,
    },
    crd::v1alpha1,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("failed to build hive-site.xml"))]
    BuildHiveSite { source: hive_site::Error },

    #[snafu(display("failed to serialize {}", ConfigFileName::Security))]
    WriteSecurityProperties { source: PropertiesWriterError },

    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to build metadata"))]
    MetadataBuild {
        source: stackable_operator::builder::meta::Error,
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
    cluster: &ValidatedCluster,
    rolegroup: &RoleGroupRef<v1alpha1::HiveCluster>,
    rg: &HiveRoleGroupConfig,
) -> Result<ConfigMap> {
    // hive-site.xml
    let hive_site_overrides = resolved_overrides(rg.config_overrides.hive_site_xml.clone());
    let hive_site_data = hive_site::build(
        &cluster.cluster_config,
        &cluster.image.product_version,
        &rg.config,
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
                .name_and_namespace(cluster)
                .name(rolegroup.object_name())
                .ownerreference_from_resource(cluster, None, Some(true))
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
                .with_recommended_labels(&build_recommended_labels(
                    cluster,
                    &cluster.image.app_version_label_value,
                    &rolegroup.role,
                    &rolegroup.role_group,
                ))
                .context(MetadataBuildSnafu)?
                .build(),
        )
        .add_data(
            ConfigFileName::HiveSite.to_string(),
            to_hadoop_xml(hive_site_data.iter()),
        )
        .add_data(
            ConfigFileName::Security.to_string(),
            to_java_properties_string(security_data.iter())
                .context(WriteSecurityPropertiesSnafu)?,
        );

    // core-site.xml is only required when Kerberos is enabled without an HDFS backend.
    if let Some(core_site_data) = core_site::build(&cluster.cluster_config) {
        cm_builder.add_data(
            ConfigFileName::CoreSite.to_string(),
            to_hadoop_xml(core_site_data.iter()),
        );
    }

    if let Some(log4j2_properties) = logging::build_log4j2(&rg.config.logging) {
        cm_builder.add_data(ConfigFileName::Log4j2.to_string(), log4j2_properties);
    }
    if let Some(vector_config) = logging::build_vector_config(rolegroup, &rg.config.logging) {
        cm_builder.add_data(VECTOR_CONFIG_FILE, vector_config);
    }

    cm_builder.build().with_context(|_| AssembleSnafu {
        rolegroup: rolegroup.clone(),
    })
}

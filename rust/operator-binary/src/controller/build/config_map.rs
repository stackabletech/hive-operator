//! Build the per-rolegroup `ConfigMap` for the Hive metastore.

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    k8s_openapi::api::core::v1::ConfigMap,
    product_logging::framework::VECTOR_CONFIG_FILE,
    v2::{
        builder::meta::ownerreference_from_resource,
        config_file_writer::{PropertiesWriterError, to_hadoop_xml, to_java_properties_string},
    },
};

use crate::controller::{
    HiveRoleGroupConfig, RoleGroupName, ValidatedCluster,
    build::properties::{
        ConfigFileName, core_site, hive_site, product_logging, security_properties,
    },
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("failed to build hive-site.xml"))]
    BuildHiveSite { source: hive_site::Error },

    #[snafu(display("failed to serialize {}", ConfigFileName::Security))]
    WriteSecurityProperties { source: PropertiesWriterError },

    #[snafu(display("failed to assemble ConfigMap for role group {role_group}"))]
    Assemble {
        source: stackable_operator::builder::configmap::Error,
        role_group: RoleGroupName,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// The rolegroup [`ConfigMap`] configures the rolegroup based on the configuration given by the
/// administrator.
///
/// `vector_config` is the Vector agent config (`vector.yaml`) built by the caller; it is `None`
/// when the Vector agent is disabled.
pub fn build_metastore_rolegroup_config_map(
    cluster: &ValidatedCluster,
    role_group_name: &RoleGroupName,
    rg: &HiveRoleGroupConfig,
    vector_config: Option<String>,
) -> Result<ConfigMap> {
    // hive-site.xml
    let hive_site_overrides = rg.config_overrides.hive_site_xml.overrides.clone();
    let hive_site_data = hive_site::build(
        &cluster.cluster_config,
        &cluster.image.product_version,
        &rg.config,
        hive_site_overrides,
    )
    .context(BuildHiveSiteSnafu)?;

    // security.properties
    let security_overrides = rg.config_overrides.security_properties.overrides.clone();
    let security_data = security_properties::build(security_overrides);

    let mut cm_builder = ConfigMapBuilder::new();
    cm_builder
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(cluster)
                .name(
                    cluster
                        .resource_names(role_group_name)
                        .role_group_config_map()
                        .to_string(),
                )
                .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
                .with_labels(cluster.recommended_labels(role_group_name))
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

    if let Some(log4j2_properties) = product_logging::build_log4j2(&rg.config.logging) {
        cm_builder.add_data(ConfigFileName::Log4j2.to_string(), log4j2_properties);
    }
    if let Some(vector_config) = vector_config {
        cm_builder.add_data(VECTOR_CONFIG_FILE, vector_config);
    }

    cm_builder.build().with_context(|_| AssembleSnafu {
        role_group: role_group_name.clone(),
    })
}

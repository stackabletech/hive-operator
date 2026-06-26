//! Build the per-rolegroup `ConfigMap` for the Hive metastore.

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::configmap::ConfigMapBuilder,
    k8s_openapi::api::core::v1::ConfigMap,
    product_logging::framework::VECTOR_CONFIG_FILE,
    utils::cluster_info::KubernetesClusterInfo,
    v2::config_file_writer::{PropertiesWriterError, to_hadoop_xml, to_java_properties_string},
};

use crate::controller::{
    HiveRoleGroupConfig, RoleGroupName, ValidatedCluster,
    build::{
        kerberos::kerberos_config_properties,
        properties::{ConfigFileName, core_site, hive_site, product_logging, security_properties},
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
pub fn build_metastore_rolegroup_config_map(
    cluster: &ValidatedCluster,
    cluster_info: &KubernetesClusterInfo,
    role_group_name: &RoleGroupName,
    rg: &HiveRoleGroupConfig,
) -> Result<ConfigMap> {
    // Kerberos-related hive-site.xml entries (empty when Kerberos is disabled). Computed here, in
    // the build step, from the validated cluster identity and the controller's cluster info.
    let kerberos_config = if cluster.has_kerberos_enabled() {
        kerberos_config_properties(
            cluster.name.as_ref(),
            cluster.namespace.as_ref(),
            cluster_info,
        )
    } else {
        Default::default()
    };

    // hive-site.xml
    let hive_site_overrides = rg.config_overrides.hive_site_xml.overrides.clone();
    let hive_site_data = hive_site::build(
        &cluster.cluster_config,
        &cluster.image.product_version,
        &rg.config,
        kerberos_config,
        hive_site_overrides,
    )
    .context(BuildHiveSiteSnafu)?;

    // security.properties
    let security_overrides = rg.config_overrides.security_properties.overrides.clone();
    let security_data = security_properties::build(security_overrides);

    let mut cm_builder = ConfigMapBuilder::new();
    cm_builder
        .metadata(
            cluster
                .object_meta(
                    cluster
                        .resource_names(role_group_name)
                        .role_group_config_map()
                        .to_string(),
                    role_group_name,
                )
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

    if let Some(log4j2_properties) =
        product_logging::build_log4j2(&rg.config.logging.hive_container)
    {
        cm_builder.add_data(ConfigFileName::Log4j2.to_string(), log4j2_properties);
    }
    if rg.config.logging.enable_vector_agent {
        cm_builder.add_data(
            VECTOR_CONFIG_FILE,
            product_logging::vector_config_file_content(),
        );
    }

    cm_builder.build().with_context(|_| AssembleSnafu {
        role_group: role_group_name.clone(),
    })
}

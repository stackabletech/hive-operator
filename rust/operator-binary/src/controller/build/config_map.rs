//! Build the per-rolegroup `ConfigMap` for the Hive metastore.

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    k8s_openapi::api::core::v1::ConfigMap,
    product_logging::framework::VECTOR_CONFIG_FILE,
    role_utils::RoleGroupRef,
};

use crate::{
    controller::{
        ValidatedCluster,
        build::properties::{
            core_site, hive_site, logging, resolved_overrides, security_properties,
        },
        build_recommended_labels,
    },
    crd::{
        CORE_SITE_XML, HIVE_METASTORE_LOG4J2_PROPERTIES, HIVE_SITE_XML, HiveRole,
        JVM_SECURITY_PROPERTIES_FILE, v1alpha1,
    },
    framework::writer::{to_hadoop_xml, to_java_properties_string},
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
    owner: &v1alpha1::HiveCluster,
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
                .name_and_namespace(owner)
                .name(rolegroup.object_name())
                .ownerreference_from_resource(owner, None, Some(true))
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
                .with_recommended_labels(&build_recommended_labels(
                    owner,
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
    if let Some(core_site_data) = core_site::build(&cluster.cluster_config) {
        cm_builder.add_data(CORE_SITE_XML, to_hadoop_xml(core_site_data.iter()));
    }

    if let Some(log4j2_properties) = logging::build_log4j2(&rg.config.logging) {
        cm_builder.add_data(HIVE_METASTORE_LOG4J2_PROPERTIES, log4j2_properties);
    }
    if let Some(vector_config) = logging::build_vector_config(rolegroup, &rg.config.logging) {
        cm_builder.add_data(VECTOR_CONFIG_FILE, vector_config);
    }

    cm_builder.build().with_context(|_| AssembleSnafu {
        rolegroup: rolegroup.clone(),
    })
}

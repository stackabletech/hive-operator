use std::{collections::BTreeMap, str::FromStr};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    commons::product_image_selection,
    config::fragment,
    product_logging::spec::Logging,
    role_utils::GenericRoleConfig,
    utils::cluster_info::KubernetesClusterInfo,
    v2::{
        builder::pod::container::{self, EnvVarName, EnvVarSet},
        controller_utils::{get_cluster_name, get_namespace, get_uid},
        product_logging::framework::{
            ValidatedContainerLogConfigChoice, VectorContainerLogConfig,
            validate_logging_configuration_for_container,
        },
        role_utils::{JavaCommonConfig, with_validated_config},
        types::kubernetes::ConfigMapName,
    },
};

use crate::{
    controller::{
        CONTAINER_IMAGE_BASE_NAME, HiveRoleGroupConfig, RoleGroupName, ValidatedCluster,
        ValidatedClusterConfig, ValidatedRoleConfig, build::kerberos::kerberos_config_properties,
        dereference::DereferencedObjects,
    },
    crd::{
        HiveRole, MetaStoreConfig,
        databases::{MetadataDatabaseConnection, derby_driver_class},
        v1alpha1::{self, HiveMetastoreRoleConfig},
    },
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to resolve product image"))]
    ResolveProductImage {
        source: product_image_selection::Error,
    },

    #[snafu(display("object defines no metastore role"))]
    NoMetaStoreRole,

    #[snafu(display("failed to resolve cluster name"))]
    ResolveClusterName {
        source: stackable_operator::v2::controller_utils::Error,
    },

    #[snafu(display("failed to resolve namespace"))]
    ResolveNamespace {
        source: stackable_operator::v2::controller_utils::Error,
    },

    #[snafu(display("failed to resolve uid"))]
    ResolveUid {
        source: stackable_operator::v2::controller_utils::Error,
    },

    #[snafu(display("invalid role group name {role_group}"))]
    ParseRoleGroupName {
        source: stackable_operator::v2::macros::attributed_string_type::Error,
        role_group: String,
    },

    #[snafu(display("failed to validate the config for role group {role_group}"))]
    ValidateConfig {
        source: fragment::ValidationError,
        role_group: RoleGroupName,
    },

    #[snafu(display("invalid environment variable override name in role group {role_group}"))]
    ParseEnvVarName {
        source: container::Error,
        role_group: RoleGroupName,
    },

    #[snafu(display("invalid metadata database connection"))]
    InvalidMetadataDatabaseConnection {
        source: stackable_operator::database_connections::Error,
    },

    #[snafu(display("failed to validate logging configuration"))]
    ValidateLoggingConfig {
        source: stackable_operator::v2::product_logging::framework::Error,
    },

    #[snafu(display(
        "the Vector aggregator discovery ConfigMap name is required when the Vector agent is enabled"
    ))]
    MissingVectorAggregatorConfigMapName,

    #[snafu(display("invalid Vector aggregator discovery ConfigMap name"))]
    ParseVectorAggregatorConfigMapName {
        source: stackable_operator::v2::macros::attributed_string_type::Error,
    },
}

/// Validated logging configuration for the Hive metastore and (optional) Vector container.
///
/// Produced up-front by [`validate_logging`] (mirroring the opensearch-operator) so that an
/// invalid custom log ConfigMap name or a missing Vector aggregator discovery ConfigMap name
/// fails reconciliation during validation rather than at resource-build time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidatedLogging {
    pub hive_container: ValidatedContainerLogConfigChoice,
    pub vector_container: Option<VectorContainerLogConfig>,
    pub enable_vector_agent: bool,
}

/// Validates the logging configuration for the Hive (and optional Vector) container.
///
/// `vector_aggregator_config_map_name` is the discovery ConfigMap name of the Vector aggregator;
/// it is required (and validated) only when the Vector agent is enabled.
fn validate_logging(
    logging: &Logging<crate::crd::Container>,
    vector_aggregator_config_map_name: &Option<ConfigMapName>,
) -> Result<ValidatedLogging, Error> {
    use crate::crd::Container;

    let hive_container = validate_logging_configuration_for_container(logging, &Container::Hive)
        .context(ValidateLoggingConfigSnafu)?;

    let vector_container = if logging.enable_vector_agent {
        let vector_aggregator_config_map_name = vector_aggregator_config_map_name
            .clone()
            .context(MissingVectorAggregatorConfigMapNameSnafu)?;
        Some(VectorContainerLogConfig {
            log_config: validate_logging_configuration_for_container(logging, &Container::Vector)
                .context(ValidateLoggingConfigSnafu)?,
            vector_aggregator_config_map_name,
        })
    } else {
        None
    };

    Ok(ValidatedLogging {
        hive_container,
        vector_container,
        enable_vector_agent: logging.enable_vector_agent,
    })
}

pub fn validate_cluster(
    hive: &v1alpha1::HiveCluster,
    image_repository: &str,
    cluster_info: &KubernetesClusterInfo,
    dereferenced_objects: DereferencedObjects,
) -> Result<ValidatedCluster, Error> {
    let name = get_cluster_name(hive).context(ResolveClusterNameSnafu)?;
    let namespace = get_namespace(hive).context(ResolveNamespaceSnafu)?;
    let uid = get_uid(hive).context(ResolveUidSnafu)?;

    let image = hive
        .spec
        .image
        .resolve(
            CONTAINER_IMAGE_BASE_NAME,
            image_repository,
            crate::built_info::PKG_VERSION,
        )
        .context(ResolveProductImageSnafu)?;

    let hive_role = HiveRole::MetaStore;
    let role = hive.spec.metastore.as_ref().context(NoMetaStoreRoleSnafu)?;

    let role_config = if let Some(HiveMetastoreRoleConfig {
        common: GenericRoleConfig {
            pod_disruption_budget: pdb,
        },
        listener_class,
    }) = hive.role_config(&hive_role)
    {
        Some(ValidatedRoleConfig {
            pdb: pdb.clone(),
            listener_class: listener_class.clone(),
        })
    } else {
        None
    };

    let default_config = MetaStoreConfig::default_config(name.as_ref(), &hive_role);

    // The Vector aggregator discovery ConfigMap name (validated here so an invalid name fails
    // up-front). It is only required when the Vector agent is enabled for a role group.
    let vector_aggregator_config_map_name = hive
        .spec
        .cluster_config
        .vector_aggregator_config_map_name
        .as_deref()
        .map(ConfigMapName::from_str)
        .transpose()
        .context(ParseVectorAggregatorConfigMapNameSnafu)?;

    let mut groups: BTreeMap<RoleGroupName, HiveRoleGroupConfig> = BTreeMap::new();
    for (rg_name, rg) in &role.role_groups {
        let role_group_name =
            RoleGroupName::from_str(rg_name).with_context(|_| ParseRoleGroupNameSnafu {
                role_group: rg_name.clone(),
            })?;
        let validated_rg = validate_role_group_config(
            &role_group_name,
            rg,
            role,
            &default_config,
            &vector_aggregator_config_map_name,
        )?;
        groups.insert(role_group_name, validated_rg);
    }

    let mut role_group_configs = BTreeMap::new();
    role_group_configs.insert(hive_role, groups);

    let metadata_database_connection_details = hive
        .spec
        .cluster_config
        .metadata_database
        .jdbc_connection_details("METADATA")
        .context(InvalidMetadataDatabaseConnectionSnafu)?;

    // The Derby driver class needs special handling per product version.
    let connection_driver = match &hive.spec.cluster_config.metadata_database {
        MetadataDatabaseConnection::Derby(_) => {
            derby_driver_class(&image.product_version).to_owned()
        }
        _ => metadata_database_connection_details.driver.clone(),
    };

    // Kerberos-related `hive-site.xml` entries (empty when Kerberos is disabled).
    let kerberos_config = if hive.has_kerberos_enabled() {
        kerberos_config_properties(name.as_ref(), namespace.as_ref(), cluster_info)
    } else {
        BTreeMap::new()
    };

    // A `core-site.xml` with `hadoop.security.authentication=kerberos` is required when
    // Kerberos is enabled and there is no HDFS backend (i.e. S3).
    let needs_kerberos_core_site =
        hive.has_kerberos_enabled() && hive.spec.cluster_config.hdfs.is_none();

    Ok(ValidatedCluster::new(
        name,
        namespace,
        uid,
        image,
        role_config,
        ValidatedClusterConfig {
            metadata_database_connection_details,
            connection_driver,
            s3_connection_spec: dereferenced_objects.s3_connection_spec,
            hive_opa_config: dereferenced_objects.hive_opa_config,
            kerberos_config,
            needs_kerberos_core_site,
        },
        role_group_configs,
    ))
}

/// Merges and validates one role group into a [`HiveRoleGroupConfig`].
///
/// Uses the upstream [`with_validated_config`] (which merges the config fragment, the
/// `configOverrides`, the `envOverrides`, the `podOverrides` and the product-specific
/// [`JavaCommonConfig`] — including its `jvmArgumentOverrides`). The merged `envOverrides`
/// (`HashMap`) are converted into an [`EnvVarSet`] here so invalid names fail validation
/// early (the opensearch-operator pattern).
fn validate_role_group_config(
    role_group_name: &RoleGroupName,
    role_group: &crate::crd::HiveRoleGroupType,
    role: &crate::crd::HiveRoleType,
    default_config: &crate::crd::MetaStoreConfigFragment,
    vector_aggregator_config_map_name: &Option<ConfigMapName>,
) -> Result<HiveRoleGroupConfig, Error> {
    let merged = with_validated_config::<
        MetaStoreConfig,
        JavaCommonConfig,
        crate::crd::MetaStoreConfigFragment,
        HiveMetastoreRoleConfig,
        v1alpha1::HiveConfigOverrides,
    >(role_group, role, default_config)
    .with_context(|_| ValidateConfigSnafu {
        role_group: role_group_name.clone(),
    })?;

    let mut env_overrides = EnvVarSet::new();
    for (env_var_name, env_var_value) in merged.config.env_overrides {
        env_overrides = env_overrides.with_value(
            &EnvVarName::from_str(&env_var_name).with_context(|_| ParseEnvVarNameSnafu {
                role_group: role_group_name.clone(),
            })?,
            env_var_value,
        );
    }

    let logging = validate_logging(
        &merged.config.config.logging,
        vector_aggregator_config_map_name,
    )?;

    Ok(HiveRoleGroupConfig {
        replicas: merged.replicas.unwrap_or(1),
        config: merged.config.config,
        config_overrides: merged.config.config_overrides,
        env_overrides,
        pod_overrides: merged.config.pod_overrides,
        jvm_argument_overrides: merged
            .config
            .product_specific_common_config
            .jvm_argument_overrides,
        logging,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_logging_rejects_invalid_custom_config_map_name() {
        use crate::crd::{
            ConfigMapLogConfig, Container, ContainerLogConfig, ContainerLogConfigChoice,
            CustomContainerLogConfig,
        };

        let logging = Logging {
            enable_vector_agent: false,
            containers: [(
                Container::Hive,
                ContainerLogConfig {
                    choice: Some(ContainerLogConfigChoice::Custom(CustomContainerLogConfig {
                        custom: ConfigMapLogConfig {
                            config_map: "invalid ConfigMap name".to_owned(),
                        },
                    })),
                },
            )]
            .into(),
        };

        assert!(validate_logging(&logging, &None).is_err());
    }
}

use std::{collections::BTreeMap, str::FromStr};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    commons::product_image_selection,
    config::fragment,
    product_logging::spec::Logging,
    role_utils::GenericRoleConfig,
    v2::{
        builder::pod::container::{EnvVarName, EnvVarSet},
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
        HiveRoleGroupConfig, RoleGroupName, ValidatedCluster, ValidatedClusterConfig,
        ValidatedMetaStoreConfig, ValidatedRoleConfig, dereference::DereferencedObjects,
    },
    crd::{
        HiveRole, MetaStoreConfig,
        databases::{MetadataDatabaseConnection, derby_driver_class},
        v1alpha1::{self, HiveMetastoreRoleConfig},
    },
};

pub const CONTAINER_IMAGE_BASE_NAME: &str = "hive";

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
        source: stackable_operator::v2::macros::attributed_string_type::Error,
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
}

/// Validated logging configuration for the Hive metastore and (optional) Vector container.
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

    // The Vector aggregator discovery ConfigMap name. It is only required when the Vector agent is
    // enabled for a role group; validity is already enforced by the `ConfigMapName` type on the CRD.
    let vector_aggregator_config_map_name = hive
        .spec
        .cluster_config
        .vector_aggregator_config_map_name
        .clone();

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

    let role_group_configs = BTreeMap::from([(hive_role, groups)]);

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

    // The database type passed to Hive via the `--db-type` CLI argument.
    let db_type = hive
        .spec
        .cluster_config
        .metadata_database
        .as_hive_db_type()
        .to_owned();

    let hdfs = hive.spec.cluster_config.hdfs.clone();

    let kerberos_secret_class = hive.kerberos_secret_class();

    Ok(ValidatedCluster::new(
        name,
        namespace,
        uid,
        image,
        role_config,
        ValidatedClusterConfig {
            metadata_database_connection_details,
            connection_driver,
            db_type,
            hdfs,
            s3_connection_spec: dereferenced_objects.s3_connection_spec,
            hive_opa_config: dereferenced_objects.hive_opa_config,
            kerberos_secret_class,
        },
        role_group_configs,
    ))
}

/// Merges and validates one role group into a [`HiveRoleGroupConfig`].
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
        replicas: merged.replicas,
        config: ValidatedMetaStoreConfig::from_merged(merged.config.config, logging),
        config_overrides: merged.config.config_overrides,
        env_overrides,
        // Hive does not use CLI overrides; the field is carried through the merge but unused.
        cli_overrides: merged.config.cli_overrides,
        pod_overrides: merged.config.pod_overrides,
        product_specific_common_config: merged.config.product_specific_common_config,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::test_support::{DERBY_YAML, minimal_hive};

    /// Runs the real validate step over the given fixture YAML, without unwrapping, so error
    /// cases can assert the specific [`Error`] variant.
    fn validate_yaml(yaml: &str) -> Result<ValidatedCluster, Error> {
        validate_cluster(
            &minimal_hive(yaml),
            "oci.example.org",
            DereferencedObjects {
                s3_connection_spec: None,
                hive_opa_config: None,
            },
        )
    }

    /// Unwraps the error of a failed validation ([`ValidatedCluster`] has no `Debug` impl, so
    /// `expect_err` is unavailable).
    fn expect_validate_err(yaml: &str) -> Error {
        match validate_yaml(yaml) {
            Ok(_) => panic!("expected validation to fail"),
            Err(error) => error,
        }
    }

    /// Locks the values the validate step derives from the minimal fixture — so a validation
    /// regression fails here, with a validate-shaped message, instead of surfacing as a confusing
    /// build-test failure downstream.
    #[test]
    fn validate_ok_derives_expected_values() {
        let cluster = validate_yaml(DERBY_YAML).expect("the minimal fixture validates");

        assert_eq!(cluster.name.to_string(), "simple-hive");
        assert_eq!(cluster.namespace.to_string(), "default");
        assert_eq!(
            cluster.product_version.to_string(),
            "4.0.0-stackable0.0.0-dev"
        );
        let role_group_names: Vec<String> = cluster
            .role_group_configs
            .values()
            .flat_map(|groups| groups.keys().map(ToString::to_string))
            .collect();
        assert_eq!(role_group_names, ["default"]);
    }

    #[test]
    fn validate_rejects_missing_metastore_role() {
        let yaml = DERBY_YAML
            .split("metastore:")
            .next()
            .expect("the fixture contains a metastore role");

        let error = expect_validate_err(yaml);
        assert!(matches!(error, Error::NoMetaStoreRole));
    }

    #[test]
    fn validate_rejects_invalid_role_group_name() {
        let yaml = DERBY_YAML.replace("default:", "Invalid_RG:");

        let error = expect_validate_err(&yaml);
        assert!(
            matches!(&error, Error::ParseRoleGroupName { role_group, .. } if role_group == "Invalid_RG"),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn validate_rejects_invalid_env_var_override_name() {
        // `EnvVarName` allows any printable ASCII except `=` (the Kubernetes rule), so a leading
        // digit is fine; an embedded `=` is what gets rejected.
        let yaml = DERBY_YAML.replace(
            "                replicas: 1",
            "                replicas: 1\n                envOverrides:\n                  \"BAD=NAME\": value",
        );

        let error = expect_validate_err(&yaml);
        assert!(
            matches!(&error, Error::ParseEnvVarName { role_group, .. } if role_group.as_ref() == "default"),
            "unexpected error: {error:?}"
        );
    }

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

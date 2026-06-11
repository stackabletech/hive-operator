use std::{collections::BTreeMap, str::FromStr};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    commons::product_image_selection,
    config::fragment,
    role_utils::GenericRoleConfig,
    utils::cluster_info::KubernetesClusterInfo,
    v2::{
        builder::pod::container::{self, EnvVarName, EnvVarSet},
        controller_utils::{get_cluster_name, get_namespace, get_uid},
        role_utils::{JavaCommonConfig, with_validated_config},
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

    #[snafu(display("failed to validate the config for role group {role_group}"))]
    ValidateConfig {
        source: fragment::ValidationError,
        role_group: String,
    },

    #[snafu(display("invalid environment variable override name in role group {role_group}"))]
    ParseEnvVarName {
        source: container::Error,
        role_group: String,
    },

    #[snafu(display("invalid metadata database connection"))]
    InvalidMetadataDatabaseConnection {
        source: stackable_operator::database_connections::Error,
    },
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

    let mut groups: BTreeMap<RoleGroupName, HiveRoleGroupConfig> = BTreeMap::new();
    for (rg_name, rg) in &role.role_groups {
        let validated_rg = validate_role_group_config(rg_name, rg, role, &default_config)?;
        groups.insert(rg_name.clone(), validated_rg);
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
    role_group_name: &str,
    role_group: &crate::crd::HiveRoleGroupType,
    role: &crate::crd::HiveRoleType,
    default_config: &crate::crd::MetaStoreConfigFragment,
) -> Result<HiveRoleGroupConfig, Error> {
    let merged = with_validated_config::<
        MetaStoreConfig,
        JavaCommonConfig,
        crate::crd::MetaStoreConfigFragment,
        HiveMetastoreRoleConfig,
        v1alpha1::HiveConfigOverrides,
    >(role_group, role, default_config)
    .with_context(|_| ValidateConfigSnafu {
        role_group: role_group_name.to_owned(),
    })?;

    let mut env_overrides = EnvVarSet::new();
    for (env_var_name, env_var_value) in merged.config.env_overrides {
        env_overrides = env_overrides.with_value(
            &EnvVarName::from_str(&env_var_name).with_context(|_| ParseEnvVarNameSnafu {
                role_group: role_group_name.to_owned(),
            })?,
            env_var_value,
        );
    }

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
    })
}

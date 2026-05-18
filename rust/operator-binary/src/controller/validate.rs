use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
};

use product_config::{ProductConfigManager, types::PropertyNameKind};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    commons::product_image_selection,
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
    role_utils::GenericRoleConfig,
};

use crate::{
    controller::{CONTAINER_IMAGE_BASE_NAME, ValidatedCluster},
    crd::{
        HIVE_SITE_XML, HiveRole, JVM_SECURITY_PROPERTIES_FILE, MetaStoreConfig,
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

    #[snafu(display("failed to generate product config"))]
    GenerateProductConfig {
        source: stackable_operator::product_config_utils::Error,
    },

    #[snafu(display("invalid product config"))]
    InvalidProductConfig {
        source: stackable_operator::product_config_utils::Error,
    },

    #[snafu(display("failed to resolve and merge config for role and role group"))]
    FailedToResolveConfig { source: crate::crd::Error },

    #[snafu(display("invalid metadata database connection"))]
    InvalidMetadataDatabaseConnection {
        source: stackable_operator::database_connections::Error,
    },
}

/// Per-role configuration extracted during validation.
#[derive(Clone, Debug)]
pub struct ValidatedRoleConfig {
    pub pdb: stackable_operator::commons::pdb::PdbConfig,
    pub listener_class: String,
}

/// Per-rolegroup configuration: the merged CRD config plus the product-config properties.
#[derive(Clone, Debug)]
pub struct ValidatedRoleGroupConfig {
    pub merged_config: MetaStoreConfig,
    pub product_config_properties: HashMap<PropertyNameKind, BTreeMap<String, String>>,
}

pub fn validate_cluster(
    hive: &v1alpha1::HiveCluster,
    image_repository: &str,
    product_config_manager: &ProductConfigManager,
) -> Result<ValidatedCluster, Error> {
    let resolved_product_image = hive
        .spec
        .image
        .resolve(
            CONTAINER_IMAGE_BASE_NAME,
            image_repository,
            crate::built_info::PKG_VERSION,
        )
        .context(ResolveProductImageSnafu)?;

    let role = hive.spec.metastore.as_ref().context(NoMetaStoreRoleSnafu)?;

    let validated_config = validate_all_roles_and_groups_config(
        &resolved_product_image.product_version,
        &transform_all_roles_to_config(
            hive,
            &[(
                HiveRole::MetaStore.to_string(),
                (
                    vec![
                        PropertyNameKind::Env,
                        PropertyNameKind::Cli,
                        PropertyNameKind::File(HIVE_SITE_XML.to_string()),
                        PropertyNameKind::File(JVM_SECURITY_PROPERTIES_FILE.to_string()),
                    ],
                    role.clone(),
                ),
            )]
            .into(),
        )
        .context(GenerateProductConfigSnafu)?,
        product_config_manager,
        false,
        false,
    )
    .context(InvalidProductConfigSnafu)?;

    let metastore_config = validated_config
        .get(&HiveRole::MetaStore.to_string())
        .map(Cow::Borrowed)
        .unwrap_or_default();

    let hive_role = HiveRole::MetaStore;

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

    let mut group_configs = BTreeMap::new();
    for (rolegroup_name, rolegroup_config) in metastore_config.iter() {
        let rolegroup = hive.metastore_rolegroup_ref(rolegroup_name);

        let merged_config = hive
            .merged_config(&hive_role, &rolegroup)
            .context(FailedToResolveConfigSnafu)?;

        group_configs.insert(
            rolegroup_name.clone(),
            ValidatedRoleGroupConfig {
                merged_config,
                product_config_properties: rolegroup_config.clone(),
            },
        );
    }

    let metadata_database_connection_details = hive
        .spec
        .cluster_config
        .metadata_database
        .jdbc_connection_details("METADATA")
        .context(InvalidMetadataDatabaseConnectionSnafu)?;

    Ok(ValidatedCluster {
        image: resolved_product_image,
        role_groups: group_configs,
        role_config,
        metadata_database_connection_details,
    })
}

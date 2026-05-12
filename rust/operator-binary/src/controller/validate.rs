use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
};

use product_config::{ProductConfigManager, types::PropertyNameKind};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    commons::product_image_selection::ResolvedProductImage,
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
    role_utils::GenericRoleConfig,
};

use super::dereference::DereferencedObjects;
use crate::crd::{
    HIVE_SITE_XML, HiveRole, JVM_SECURITY_PROPERTIES_FILE, MetaStoreConfig,
    v1alpha1::{self, HiveMetastoreRoleConfig},
};

#[derive(Snafu, Debug)]
pub enum Error {
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

    #[snafu(display("failed to resolve and merge resource config for role and role group"))]
    FailedToResolveResourceConfig { source: crate::crd::Error },
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

/// The validated cluster: proves that product-config validation and config merging
/// succeeded for every role and role group before any resources are created.
#[derive(Clone, Debug)]
pub struct ValidatedHiveCluster {
    pub image: ResolvedProductImage,
    pub role_groups: BTreeMap<HiveRole, BTreeMap<String, ValidatedRoleGroupConfig>>,
    pub role_configs: BTreeMap<HiveRole, ValidatedRoleConfig>,
}

pub fn validate_cluster(
    hive: &v1alpha1::HiveCluster,
    dereferenced: &DereferencedObjects,
    product_config_manager: &ProductConfigManager,
) -> Result<ValidatedHiveCluster, Error> {
    let role = hive.spec.metastore.as_ref().context(NoMetaStoreRoleSnafu)?;

    let validated_config = validate_all_roles_and_groups_config(
        &dereferenced.resolved_product_image.product_version,
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

    let mut role_groups = BTreeMap::new();
    let mut role_configs = BTreeMap::new();

    let metastore_config = validated_config
        .get(&HiveRole::MetaStore.to_string())
        .map(Cow::Borrowed)
        .unwrap_or_default();

    let hive_role = HiveRole::MetaStore;

    if let Some(HiveMetastoreRoleConfig {
        common: GenericRoleConfig {
            pod_disruption_budget: pdb,
        },
        listener_class,
    }) = hive.role_config(&hive_role)
    {
        role_configs.insert(
            hive_role.clone(),
            ValidatedRoleConfig {
                pdb: pdb.clone(),
                listener_class: listener_class.clone(),
            },
        );
    }

    let mut group_configs = BTreeMap::new();
    for (rolegroup_name, rolegroup_config) in metastore_config.iter() {
        let rolegroup = hive.metastore_rolegroup_ref(rolegroup_name);

        let merged_config = hive
            .merged_config(&hive_role, &rolegroup)
            .context(FailedToResolveResourceConfigSnafu)?;

        group_configs.insert(
            rolegroup_name.clone(),
            ValidatedRoleGroupConfig {
                merged_config,
                product_config_properties: rolegroup_config.clone(),
            },
        );
    }

    role_groups.insert(hive_role, group_configs);

    Ok(ValidatedHiveCluster {
        image: dereferenced.resolved_product_image.clone(),
        role_groups,
        role_configs,
    })
}

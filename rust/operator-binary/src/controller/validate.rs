use std::{collections::BTreeMap, str::FromStr};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    commons::product_image_selection,
    kube::ResourceExt as _,
    role_utils::{GenericRoleConfig, JavaCommonConfig},
    v2::types::operator::ClusterName,
};

use crate::{
    controller::{
        CONTAINER_IMAGE_BASE_NAME, HiveRoleGroupConfig, RoleGroupName, ValidatedCluster,
        ValidatedClusterConfig, ValidatedRoleConfig, dereference::DereferencedObjects,
    },
    crd::{
        HiveRole, MetaStoreConfig,
        v1alpha1::{self, HiveMetastoreRoleConfig},
    },
    framework::role_utils::with_validated_config,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to resolve product image"))]
    ResolveProductImage {
        source: product_image_selection::Error,
    },

    #[snafu(display("object defines no metastore role"))]
    NoMetaStoreRole,

    #[snafu(display("invalid cluster name"))]
    InvalidClusterName {
        source: stackable_operator::v2::macros::attributed_string_type::Error,
    },

    #[snafu(display("failed to resolve and merge config for role group {role_group}"))]
    FailedToResolveConfig {
        source: stackable_operator::config::fragment::ValidationError,
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
    dereferenced_objects: DereferencedObjects,
) -> Result<ValidatedCluster, Error> {
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

    let default_config = MetaStoreConfig::default_config(&hive.name_any(), &hive_role);

    let mut groups: BTreeMap<RoleGroupName, HiveRoleGroupConfig> = BTreeMap::new();
    for (rg_name, rg) in &role.role_groups {
        let validated_rg = with_validated_config::<
            MetaStoreConfig,
            JavaCommonConfig,
            crate::crd::MetaStoreConfigFragment,
            HiveMetastoreRoleConfig,
            v1alpha1::HiveConfigOverrides,
        >(rg, role, &default_config)
        .with_context(|_| FailedToResolveConfigSnafu {
            role_group: rg_name.clone(),
        })?;
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

    Ok(ValidatedCluster {
        name: ClusterName::from_str(&hive.name_any()).context(InvalidClusterNameSnafu)?,
        image,
        role_config,
        cluster_config: ValidatedClusterConfig {
            metadata_database_connection_details,
            s3_connection_spec: dereferenced_objects.s3_connection_spec,
            hive_opa_config: dereferenced_objects.hive_opa_config,
        },
        role_group_configs,
    })
}

//! Vendored variant of `stackable_operator::v2::role_utils` from the
//! `smooth-operator` branch, with simplifications appropriate for hive-operator.
//!
//! Differences from upstream:
//! - `env_overrides` is `BTreeMap<String, String>` instead of `EnvVarSet`.
//! - No `cli_overrides_to_vec` helper, `ResourceNames`, or service-account helpers.
//! - The `CommonConfig` (a.k.a. `product_specific_common_config`) does NOT need to
//!   implement `Merge`. Hive uses `JavaCommonConfig`, which intentionally does not
//!   implement `Merge` because its inner `JvmArgumentOverrides::try_merge` is
//!   fallible (regex validation). The `RoleGroupConfig::product_specific_common_config`
//!   field here simply carries the role-group level value through.
//!
//! Replace with `stackable_operator::v2::role_utils::*` once upstream relaxes the
//! `Merge` bound.

use std::collections::BTreeMap;

use serde::Serialize;
use stackable_operator::{
    config::{
        fragment::{self, FromFragment},
        merge::{Merge, merge},
    },
    k8s_openapi::{DeepMerge, api::core::v1::PodTemplateSpec},
    role_utils::{Role, RoleGroup},
    schemars::JsonSchema,
};

/// Hive-friendly view of a validated, merged `RoleGroup`.
#[derive(Clone, Debug, PartialEq)]
pub struct RoleGroupConfig<Config, CommonConfig, ConfigOverrides> {
    pub replicas: u16,
    pub config: Config,
    pub config_overrides: ConfigOverrides,
    pub env_overrides: BTreeMap<String, String>,
    pub cli_overrides: BTreeMap<String, String>,
    pub pod_overrides: PodTemplateSpec,
    pub product_specific_common_config: CommonConfig,
}

/// Merges and validates the `RoleGroup` with the given `role` and `default_config`.
pub fn with_validated_config<ValidatedConfig, CommonConfig, Config, RoleConfig, ConfigOverrides>(
    role_group: &RoleGroup<Config, CommonConfig, ConfigOverrides>,
    role: &Role<Config, ConfigOverrides, RoleConfig, CommonConfig>,
    default_config: &Config,
) -> Result<
    RoleGroupConfig<ValidatedConfig, CommonConfig, ConfigOverrides>,
    fragment::ValidationError,
>
where
    ValidatedConfig: FromFragment<Fragment = Config>,
    CommonConfig: Clone + Default + JsonSchema + Serialize,
    Config: Clone + Merge,
    RoleConfig: Default + JsonSchema + Serialize,
    ConfigOverrides: Clone + Default + JsonSchema + Merge + Serialize,
{
    let validated_config = validate_config(role_group, role, default_config)?;
    Ok(RoleGroupConfig {
        replicas: role_group.replicas.unwrap_or(1),
        config: validated_config,
        config_overrides: merged_config_overrides(
            &role.config.config_overrides,
            role_group.config.config_overrides.clone(),
        ),
        env_overrides: merged_env_overrides(
            role.config
                .env_overrides
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            role_group
                .config
                .env_overrides
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        ),
        cli_overrides: merged_cli_overrides(
            role.config.cli_overrides.clone(),
            role_group.config.cli_overrides.clone(),
        ),
        pod_overrides: merged_pod_overrides(
            role.config.pod_overrides.clone(),
            role_group.config.pod_overrides.clone(),
        ),
        product_specific_common_config: role_group.config.product_specific_common_config.clone(),
    })
}

fn validate_config<ValidatedConfig, CommonConfig, Config, RoleConfig, ConfigOverrides>(
    role_group: &RoleGroup<Config, CommonConfig, ConfigOverrides>,
    role: &Role<Config, ConfigOverrides, RoleConfig, CommonConfig>,
    default_config: &Config,
) -> Result<ValidatedConfig, fragment::ValidationError>
where
    ValidatedConfig: FromFragment<Fragment = Config>,
    CommonConfig: Default + JsonSchema + Serialize,
    Config: Clone + Merge,
    RoleConfig: Default + JsonSchema + Serialize,
    ConfigOverrides: Default + JsonSchema + Serialize,
{
    role_group.validate_config(role, default_config)
}

fn merged_config_overrides<ConfigOverrides>(
    role_config_overrides: &ConfigOverrides,
    role_group_config_overrides: ConfigOverrides,
) -> ConfigOverrides
where
    ConfigOverrides: Merge,
{
    merge(role_group_config_overrides, role_config_overrides)
}

fn merged_env_overrides(
    role_env_overrides: BTreeMap<String, String>,
    role_group_env_overrides: BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut merged = role_env_overrides;
    merged.extend(role_group_env_overrides);
    merged
}

fn merged_cli_overrides(
    role_cli_overrides: BTreeMap<String, String>,
    role_group_cli_overrides: BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut merged = role_cli_overrides;
    merged.extend(role_group_cli_overrides);
    merged
}

fn merged_pod_overrides(
    role_pod_overrides: PodTemplateSpec,
    role_group_pod_overrides: PodTemplateSpec,
) -> PodTemplateSpec {
    let mut merged = role_pod_overrides;
    merged.merge_from(role_group_pod_overrides);
    merged
}

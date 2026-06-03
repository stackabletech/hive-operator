//! Local framework helpers that mirror the work-in-progress upstream
//! `stackable_operator::v2::*` modules.
//!
//! We vendor `role_utils` because the upstream `v2::role_utils` requires
//! `CommonConfig: Merge` and uses `EnvVarSet` for `env_overrides`. Hive (like
//! trino) uses `JavaCommonConfig`, whose JVM-argument merge is fallible and so
//! does not implement `Merge`; we also want `env_overrides` as a plain
//! `BTreeMap<String, String>`.
//!
//! Follow-up: replace with `stackable_operator::v2::role_utils::*` once upstream
//! relaxes the `Merge` bound.

pub mod role_utils;

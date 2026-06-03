//! Per-file builders for the Hive metastore config files.

use std::collections::BTreeMap;

use stackable_operator::v2::config_overrides::KeyValueConfigOverrides;

pub mod core_site;
pub mod hive_site;
pub mod security_properties;

/// Resolve user-provided key/value overrides into `(key, value)` pairs, dropping
/// entries whose value is unset (`None`).
pub(crate) fn resolved_overrides(overrides: KeyValueConfigOverrides) -> BTreeMap<String, String> {
    overrides
        .overrides
        .into_iter()
        .filter_map(|(key, value)| value.map(|value| (key, value)))
        .collect()
}

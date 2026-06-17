use std::{collections::BTreeMap, str::FromStr};

use stackable_operator::v2::types::kubernetes::VolumeName;

use crate::controller::dereference::ResolvedOpaConfig;

const HIVE_METASTORE_PRE_EVENT_LISTENERS: &str = "hive.metastore.pre.event.listeners";
const HIVE_SECURITY_METASTORE_AUTHORIZATION_MANAGER: &str =
    "hive.security.metastore.authorization.manager";

const OPA_AUTHORIZATION_PRE_EVENT_LISTENER_V3: &str =
    "com.bosch.bdps.hms3.OpaAuthorizationPreEventListener";
const OPA_BASED_AUTHORIZATION_PROVIDER_V3: &str =
    "com.bosch.bdps.hms3.OpaBasedAuthorizationProvider";
const OPA_AUTHORIZATION_PRE_EVENT_LISTENER_V4: &str =
    "com.bosch.bdps.hms4.OpaAuthorizationPreEventListener";
const OPA_BASED_AUTHORIZATION_PROVIDER_V4: &str =
    "com.bosch.bdps.hms4.OpaBasedAuthorizationProvider";

const OPA_AUTHORIZATION_BASE_ENDPOINT: &str = "com.bosch.bdps.opa.authorization.base.endpoint";
const OPA_AUTHORIZATION_POLICY_URL_DATA_BASE: &str =
    "com.bosch.bdps.opa.authorization.policy.url.database";
const OPA_AUTHORIZATION_POLICY_URL_TABLE: &str =
    "com.bosch.bdps.opa.authorization.policy.url.table";
const OPA_AUTHORIZATION_POLICY_URL_COLUMN: &str =
    "com.bosch.bdps.opa.authorization.policy.url.column";
const OPA_AUTHORIZATION_POLICY_URL_PARTITION: &str =
    "com.bosch.bdps.opa.authorization.policy.url.partition";
const OPA_AUTHORIZATION_POLICY_URL_USER: &str = "com.bosch.bdps.opa.authorization.policy.url.user";

// Typed name for the OPA TLS secret-operator volume, reusing the existing `"opa-tls"` string
// value so the produced volume/mount name is unchanged.
stackable_operator::constant!(pub(crate) OPA_TLS_VOLUME_NAME: VolumeName = "opa-tls");

/// Builds the OPA-related `hive-site.xml` properties from a [`ResolvedOpaConfig`].
pub fn build_opa_hive_site_config(
    opa: &ResolvedOpaConfig,
    product_version: &str,
) -> BTreeMap<String, String> {
    let (pre_event_listener, authorization_provider) = if product_version.starts_with("3.") {
        (
            OPA_AUTHORIZATION_PRE_EVENT_LISTENER_V3,
            OPA_BASED_AUTHORIZATION_PROVIDER_V3,
        )
    } else {
        (
            OPA_AUTHORIZATION_PRE_EVENT_LISTENER_V4,
            OPA_BASED_AUTHORIZATION_PROVIDER_V4,
        )
    };

    BTreeMap::from([
        (
            HIVE_METASTORE_PRE_EVENT_LISTENERS.to_string(),
            pre_event_listener.to_string(),
        ),
        (
            HIVE_SECURITY_METASTORE_AUTHORIZATION_MANAGER.to_string(),
            authorization_provider.to_string(),
        ),
        (
            OPA_AUTHORIZATION_BASE_ENDPOINT.to_string(),
            opa.base_endpoint.to_owned(),
        ),
        (
            OPA_AUTHORIZATION_POLICY_URL_DATA_BASE.to_string(),
            "database_allow".to_string(),
        ),
        (
            OPA_AUTHORIZATION_POLICY_URL_TABLE.to_string(),
            "table_allow".to_string(),
        ),
        (
            OPA_AUTHORIZATION_POLICY_URL_COLUMN.to_string(),
            "column_allow".to_string(),
        ),
        (
            OPA_AUTHORIZATION_POLICY_URL_PARTITION.to_string(),
            "partition_allow".to_string(),
        ),
        (
            OPA_AUTHORIZATION_POLICY_URL_USER.to_string(),
            "user_allow".to_string(),
        ),
    ])
}

/// The mount path of the OPA TLS CA certificate, or `None` when OPA TLS is not configured.
pub fn build_opa_tls_ca_cert_mount_path(opa: &ResolvedOpaConfig) -> Option<String> {
    opa.tls_secret_class
        .as_ref()
        .map(|_| format!("/stackable/secrets/{}", *OPA_TLS_VOLUME_NAME))
}

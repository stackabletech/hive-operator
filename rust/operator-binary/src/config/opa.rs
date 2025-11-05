use std::collections::BTreeMap;

use stackable_operator::{
    client::Client,
    commons::opa::{OpaApiVersion, OpaConfig},
    k8s_openapi::api::core::v1::ConfigMap,
    kube::ResourceExt,
};

use crate::crd::v1alpha1::HiveCluster;

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

pub const OPA_TLS_VOLUME_NAME: &str = "opa-tls";

pub struct HiveOpaConfig {
    /// Endpoint for OPA, e.g.
    /// `http://localhost:8081/v1/data/hms/allow`
    pub(crate) base_endpoint: String,
    /// Policy to check database authorization, e.g.
    /// `http://localhost:8081/v1/data/hms/database_allow`
    pub(crate) policy_url_database: String,
    /// Policy to check table authorization, e.g.
    /// `http://localhost:8081/v1/data/hms/table_allow`
    pub(crate) policy_url_table: String,
    /// Policy to check column authorization, e.g.
    /// `http://localhost:8081/v1/data/hms/column_allow`
    pub(crate) policy_url_column: String,
    /// Policy to check partition authorization, e.g.
    /// `http://localhost:8081/v1/data/hms/partition_allow`
    pub(crate) policy_url_partition: String,
    /// Policy to check user authorization, e.g.
    /// `http://localhost:8081/v1/data/hms/user_allow`
    pub(crate) policy_url_user: String,
    /// Optional TLS secret class for OPA communication.
    /// If set, the CA certificate from this secret class will be added
    /// to hive's truststore to make it trust OPA's TLS certificate.
    pub(crate) tls_secret_class: Option<String>,
}

impl HiveOpaConfig {
    pub async fn from_opa_config(
        client: &Client,
        hive: &HiveCluster,
        opa_config: &OpaConfig,
    ) -> Result<Self, stackable_operator::commons::opa::Error> {
        // See: https://github.com/boschglobal/hive-metastore-opa-authorizer?tab=readme-ov-file#configuration
        // TODO: get document root once (client call) and build the other strings
        let base_endpoint = opa_config
            .full_document_url_from_config_map(client, hive, Some("allow"), OpaApiVersion::V1)
            .await?;

        let policy_url_database = opa_config
            .full_document_url_from_config_map(
                client,
                hive,
                Some("database_allow"),
                OpaApiVersion::V1,
            )
            .await?;
        let policy_url_table = opa_config
            .full_document_url_from_config_map(client, hive, Some("table_allow"), OpaApiVersion::V1)
            .await?;
        let policy_url_column = opa_config
            .full_document_url_from_config_map(
                client,
                hive,
                Some("column_allow"),
                OpaApiVersion::V1,
            )
            .await?;
        let policy_url_partition = opa_config
            .full_document_url_from_config_map(
                client,
                hive,
                Some("partition_allow"),
                OpaApiVersion::V1,
            )
            .await?;
        let policy_url_user = opa_config
            .full_document_url_from_config_map(client, hive, Some("user_allow"), OpaApiVersion::V1)
            .await?;

        let tls_secret_class = client
            .get::<ConfigMap>(
                &opa_config.config_map_name,
                hive.namespace().as_deref().unwrap_or("default"),
            )
            .await
            .ok()
            .and_then(|cm| cm.data)
            .and_then(|mut data| data.remove("OPA_SECRET_CLASS"));

        Ok(HiveOpaConfig {
            base_endpoint,
            policy_url_database,
            policy_url_table,
            policy_url_column,
            policy_url_partition,
            policy_url_user,
            tls_secret_class,
        })
    }

    pub fn as_config(&self, product_version: &str) -> BTreeMap<String, String> {
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
                self.base_endpoint.to_owned(),
            ),
            (
                OPA_AUTHORIZATION_POLICY_URL_DATA_BASE.to_string(),
                self.policy_url_database.to_owned(),
            ),
            (
                OPA_AUTHORIZATION_POLICY_URL_TABLE.to_string(),
                self.policy_url_table.to_owned(),
            ),
            (
                OPA_AUTHORIZATION_POLICY_URL_COLUMN.to_string(),
                self.policy_url_column.to_owned(),
            ),
            (
                OPA_AUTHORIZATION_POLICY_URL_PARTITION.to_string(),
                self.policy_url_partition.to_owned(),
            ),
            (
                OPA_AUTHORIZATION_POLICY_URL_USER.to_string(),
                self.policy_url_user.to_owned(),
            ),
        ])
    }

    pub fn tls_mount_path(&self) -> Option<String> {
        self.tls_secret_class
            .as_ref()
            .map(|_| format!("/stackable/secrets/{OPA_TLS_VOLUME_NAME}"))
    }
}

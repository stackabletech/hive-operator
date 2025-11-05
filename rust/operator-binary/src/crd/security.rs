use serde::{Deserialize, Serialize};
use stackable_operator::{
    commons::opa::OpaConfig,
    schemars::{self, JsonSchema},
};

#[derive(Clone, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthenticationConfig {
    /// Kerberos configuration.
    pub kerberos: KerberosConfig,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthorizationConfig {
    // no doc - it's in the struct.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opa: Option<OpaConfig>,
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KerberosConfig {
    /// Name of the SecretClass providing the keytab for the HBase services.
    pub secret_class: String,
}

use serde::{Deserialize, Serialize};
use stackable_operator::{
    commons::opa::OpaConfig,
    schemars::{self, JsonSchema},
    v2::types::kubernetes::SecretClassName,
};
use url::Url;

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThriftAuthenticationConfig {
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

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KerberosConfig {
    /// Name of the SecretClass providing the keytab for the HBase services.
    pub secret_class: SecretClassName,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum IcebergRestCatalogAuthenticationConfig {
    // /// Hive Metastore assumes that the value of the x-actor-username HTTP header is the
    // /// authenticated username. You may use it to test authorized access, but this mode is not
    // /// recommend in a production environment.
    // Simple(()),
    /// OAuth 2 is the industry standard for authenticating Iceberg client usernames. You can
    /// integrate Hive Metastore with your Authorization Server, e.g., Keycloak, to protect Iceberg
    /// resources.
    OAuth2(OAuth2Config),
    // /// Configure Hive Metastore so that its Iceberg REST API accepts a JSON Web Token (JWT) as a
    // /// bearer token in the Authorization header. The JSON Web Key Set (JWKS) locations must be
    // /// configured using the metastore.authentication.jwt.jwks.url property. Hive Metastore derives
    // /// the username from the sub claim of a properly-signed JWT.
    // Jwt(()),
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OAuth2Config {
    pub issuer_url: Url,

    pub audience: String,
}

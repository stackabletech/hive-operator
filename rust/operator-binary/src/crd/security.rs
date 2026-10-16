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
    /// Name of the SecretClass providing the keytab for the Hive metastore.
    pub secret_class: SecretClassName,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum IcebergRestCatalogAuthenticationConfig {
    /// Hive Metastore assumes that the value of the `x-actor-username` HTTP header is the
    /// authenticated username, without verifying it. You may use it to test authorized access,
    /// but this mode is not recommended in a production environment.
    Simple {},
    /// OAuth 2 is the industry standard for authenticating Iceberg client usernames. You can
    /// integrate Hive Metastore with your Authorization Server, e.g., Keycloak, to protect Iceberg
    /// resources.
    OAuth2(OAuth2AuthenticationConfig),
    /// Configure Hive Metastore so that its Iceberg REST API accepts a JSON Web Token (JWT) as a
    /// bearer token in the Authorization header. Hive Metastore derives the username from the `sub`
    /// claim of a properly-signed JWT.
    ///
    /// Only the signature and the expiry of the JWT are checked, not its issuer or audience.
    /// Prefer `oAuth2` where possible.
    Jwt(JwtAuthenticationConfig),
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OAuth2AuthenticationConfig {
    /// URL of the OAuth 2 authorization server (issuer), e.g. a Keycloak realm.
    ///
    /// The Hive metastore fetches the server metadata from
    /// `<issuerUrl>/.well-known/oauth-authorization-server` on startup, which must include the
    /// `jwks_uri` used to validate access tokens.
    pub issuer_url: Url,

    /// The audience the JWT access tokens must be issued for.
    pub audience: String,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JwtAuthenticationConfig {
    /// URLs of the JSON Web Key Set (JWKS) used to validate the signature of the JWTs.
    ///
    /// Maps to the `metastore.authentication.jwt.jwks.url` setting. Hive only falls back to
    /// another URL if fetching the JWKS fails, so all URLs need to serve the same keys. Hive does
    /// not preserve the order of the URLs, and the URLs must not contain commas.
    #[schemars(length(min = 1))]
    pub jwks_urls: Vec<Url>,
}

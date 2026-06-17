use std::str::FromStr;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    client::Client,
    commons::opa::{OpaApiVersion, OpaConfig},
    crd::s3,
    k8s_openapi::api::core::v1::ConfigMap,
    kube::ResourceExt,
    v2::{controller_utils::get_namespace, types::kubernetes::SecretClassName},
};

use crate::crd::v1alpha1;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to resolve namespace"))]
    ResolveNamespace {
        source: stackable_operator::v2::controller_utils::Error,
    },

    #[snafu(display("failed to configure S3 connection"))]
    ConfigureS3Connection {
        source: s3::v1alpha1::ConnectionError,
    },

    #[snafu(display("invalid OPA configuration"))]
    InvalidOpaConfig {
        source: stackable_operator::commons::opa::Error,
    },

    #[snafu(display("invalid OPA TLS secret class name"))]
    ParseOpaTlsSecretClassName {
        source: stackable_operator::v2::macros::attributed_string_type::Error,
    },
}

/// External references resolved during the dereference step.
pub struct DereferencedObjects {
    pub s3_connection_spec: Option<s3::v1alpha1::ConnectionSpec>,
    pub hive_opa_config: Option<ResolvedOpaConfig>,
}

/// OPA settings resolved from the cluster's OPA reference during the dereference step.
pub struct ResolvedOpaConfig {
    /// Endpoint for OPA, e.g.
    /// `http://localhost:8081/v1/data/<package>`
    pub(crate) base_endpoint: String,
    /// Optional TLS secret class for OPA communication.
    /// If set, the CA certificate from this secret class will be added
    /// to hive's truststore to make it trust OPA's TLS certificate.
    pub(crate) tls_secret_class: Option<SecretClassName>,
}

impl ResolvedOpaConfig {
    pub async fn from_opa_config(
        client: &Client,
        hive: &v1alpha1::HiveCluster,
        opa_config: &OpaConfig,
    ) -> Result<Self, Error> {
        // See: <https://github.com/boschglobal/hive-metastore-opa-authorizer?tab=readme-ov-file#configuration>
        let base_endpoint = opa_config
            .full_document_url_from_config_map(client, hive, None, &OpaApiVersion::V1)
            .await
            .context(InvalidOpaConfigSnafu)?;

        let tls_secret_class = client
            .get::<ConfigMap>(
                &opa_config.config_map_name,
                hive.namespace().as_deref().unwrap_or("default"),
            )
            .await
            .ok()
            .and_then(|cm| cm.data)
            .and_then(|mut data| data.remove("OPA_SECRET_CLASS"))
            .map(|name| SecretClassName::from_str(&name))
            .transpose()
            .context(ParseOpaTlsSecretClassNameSnafu)?;

        Ok(ResolvedOpaConfig {
            base_endpoint,
            tls_secret_class,
        })
    }
}

pub async fn dereference(
    client: &stackable_operator::client::Client,
    hive: &v1alpha1::HiveCluster,
) -> Result<DereferencedObjects, Error> {
    let s3_connection_spec: Option<s3::v1alpha1::ConnectionSpec> =
        if let Some(s3) = &hive.spec.cluster_config.s3 {
            let namespace = get_namespace(hive).context(ResolveNamespaceSnafu)?;
            Some(
                s3.clone()
                    .resolve(client, namespace.as_ref())
                    .await
                    .context(ConfigureS3ConnectionSnafu)?,
            )
        } else {
            None
        };

    let hive_opa_config = match hive.get_opa_config() {
        Some(opa_config) => {
            Some(ResolvedOpaConfig::from_opa_config(client, hive, opa_config).await?)
        }
        None => None,
    };

    Ok(DereferencedObjects {
        s3_connection_spec,
        hive_opa_config,
    })
}

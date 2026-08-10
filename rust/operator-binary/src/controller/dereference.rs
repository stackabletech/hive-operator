use std::str::FromStr;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    client::Client,
    commons::opa::{OpaApiVersion, OpaConfig},
    crd::{listener::v1alpha1::Listener, s3},
    k8s_openapi::api::core::v1::ConfigMap,
    v2::{
        controller_utils::{get_cluster_name, get_namespace},
        types::kubernetes::SecretClassName,
    },
};

use crate::{
    controller::build::resource::{
        discovery::discovery_config_map_name, listener::role_listener_name,
    },
    crd::{HiveRole, v1alpha1},
};

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

    #[snafu(display("failed to determine the cluster's name"))]
    ResolveClusterName {
        source: stackable_operator::v2::controller_utils::Error,
    },

    #[snafu(display("failed to get the metastore role Listener {listener_name}"))]
    GetRoleListener {
        source: stackable_operator::client::Error,
        listener_name: String,
    },

    #[snafu(display("failed to get the existing discovery ConfigMap {config_map_name}"))]
    GetExistingDiscoveryConfigMap {
        source: stackable_operator::client::Error,
        config_map_name: String,
    },
}

/// External references resolved during the dereference step.
pub struct DereferencedObjects {
    pub s3_connection_spec: Option<s3::v1alpha1::ConnectionSpec>,
    pub hive_opa_config: Option<ResolvedOpaConfig>,
    /// The metastore role [`Listener`] as currently stored in the cluster, fetched because the
    /// discovery `ConfigMap` is built from its ingress address. Unlike the other fields it is not
    /// referenced from the spec but created by this operator itself: `None` on the first
    /// reconcile run (the apply step has not created it yet), and its status is only populated
    /// asynchronously by the listener-operator, so it can still be address-less here.
    pub role_listener: Option<Listener>,

    /// The discovery `ConfigMap` as currently stored in the cluster (named by
    /// [`discovery_config_map_name`]), fetched so that the build step can re-emit it while the
    /// role Listener yields no ingress address to build a fresh one from. `None` before the
    /// first successful build.
    pub existing_discovery_config_map: Option<ConfigMap>,
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

        let namespace = get_namespace(hive).context(ResolveNamespaceSnafu)?;
        let tls_secret_class = client
            .get::<ConfigMap>(&opa_config.config_map_name, namespace.as_ref())
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

    let cluster_name = get_cluster_name(hive).context(ResolveClusterNameSnafu)?;
    let namespace = get_namespace(hive).context(ResolveNamespaceSnafu)?;
    let listener_name = role_listener_name(&cluster_name, &HiveRole::MetaStore);
    let role_listener = client
        .get_opt::<Listener>(listener_name.as_ref(), namespace.as_ref())
        .await
        .context(GetRoleListenerSnafu {
            listener_name: listener_name.as_ref(),
        })?;

    let discovery_config_map_name = discovery_config_map_name(&cluster_name);
    let existing_discovery_config_map = client
        .get_opt::<ConfigMap>(discovery_config_map_name.as_ref(), namespace.as_ref())
        .await
        .context(GetExistingDiscoveryConfigMapSnafu {
            config_map_name: discovery_config_map_name.as_ref(),
        })?;

    Ok(DereferencedObjects {
        s3_connection_spec,
        hive_opa_config,
        role_listener,
        existing_discovery_config_map,
    })
}

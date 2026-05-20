use snafu::{ResultExt, Snafu};
use stackable_operator::{crd::s3, kube::ResourceExt};

use crate::{config::opa::HiveOpaConfig, crd::v1alpha1};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object defines no namespace"))]
    ObjectHasNoNamespace,

    #[snafu(display("failed to configure S3 connection"))]
    ConfigureS3Connection {
        source: s3::v1alpha1::ConnectionError,
    },

    #[snafu(display("invalid OPA configuration"))]
    InvalidOpaConfig {
        source: stackable_operator::commons::opa::Error,
    },
}

/// External references resolved during the dereference step.
pub struct DereferencedObjects {
    pub s3_connection_spec: Option<s3::v1alpha1::ConnectionSpec>,
    pub hive_opa_config: Option<HiveOpaConfig>,
}

pub async fn dereference(
    client: &stackable_operator::client::Client,
    hive: &v1alpha1::HiveCluster,
) -> Result<DereferencedObjects, Error> {
    let s3_connection_spec: Option<s3::v1alpha1::ConnectionSpec> =
        if let Some(s3) = &hive.spec.cluster_config.s3 {
            Some(
                s3.clone()
                    .resolve(
                        client,
                        &hive.namespace().ok_or(Error::ObjectHasNoNamespace)?,
                    )
                    .await
                    .context(ConfigureS3ConnectionSnafu)?,
            )
        } else {
            None
        };

    let hive_opa_config = match hive.get_opa_config() {
        Some(opa_config) => Some(
            HiveOpaConfig::from_opa_config(client, hive, opa_config)
                .await
                .context(InvalidOpaConfigSnafu)?,
        ),
        None => None,
    };

    Ok(DereferencedObjects {
        s3_connection_spec,
        hive_opa_config,
    })
}

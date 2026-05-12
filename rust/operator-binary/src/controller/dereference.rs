use snafu::{ResultExt, Snafu};
use stackable_operator::{
    commons::product_image_selection::{self, ResolvedProductImage},
    crd::s3,
    database_connections::drivers::jdbc::JdbcDatabaseConnectionDetails,
    kube::ResourceExt,
};

use crate::{config::opa::HiveOpaConfig, crd::v1alpha1};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to resolve product image"))]
    ResolveProductImage {
        source: product_image_selection::Error,
    },

    #[snafu(display("object defines no namespace"))]
    ObjectHasNoNamespace,

    #[snafu(display("failed to configure S3 connection"))]
    ConfigureS3Connection {
        source: s3::v1alpha1::ConnectionError,
    },

    #[snafu(display("invalid metadata database connection"))]
    InvalidMetadataDatabaseConnection {
        source: stackable_operator::database_connections::Error,
    },

    #[snafu(display("invalid OpaConfig"))]
    InvalidOpaConfig {
        source: stackable_operator::commons::opa::Error,
    },
}

/// External references resolved during the dereference step.
pub struct DereferencedObjects {
    pub resolved_product_image: ResolvedProductImage,
    pub s3_connection_spec: Option<s3::v1alpha1::ConnectionSpec>,
    pub metadata_database_connection_details: JdbcDatabaseConnectionDetails,
    pub hive_opa_config: Option<HiveOpaConfig>,
}

pub async fn dereference(
    client: &stackable_operator::client::Client,
    hive: &v1alpha1::HiveCluster,
    image_base_name: &str,
    image_repository: &str,
    pkg_version: &str,
) -> Result<DereferencedObjects, Error> {
    let resolved_product_image = hive
        .spec
        .image
        .resolve(image_base_name, image_repository, pkg_version)
        .context(ResolveProductImageSnafu)?;

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

    let metadata_database_connection_details = hive
        .spec
        .cluster_config
        .metadata_database
        .jdbc_connection_details("METADATA")
        .context(InvalidMetadataDatabaseConnectionSnafu)?;

    let hive_opa_config = match hive.get_opa_config() {
        Some(opa_config) => Some(
            HiveOpaConfig::from_opa_config(client, hive, opa_config)
                .await
                .context(InvalidOpaConfigSnafu)?,
        ),
        None => None,
    };

    Ok(DereferencedObjects {
        resolved_product_image,
        s3_connection_spec,
        metadata_database_connection_details,
        hive_opa_config,
    })
}

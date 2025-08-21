use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    commons::product_image_selection::ResolvedProductImage,
    k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec},
    kvp::{Label, Labels},
    role_utils::RoleGroupRef,
};

use crate::{
    controller::build_recommended_labels,
    crd::{APP_NAME, HIVE_PORT, HIVE_PORT_NAME, METRICS_PORT, METRICS_PORT_NAME, v1alpha1},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
    },
    #[snafu(display("failed to build Metadata"))]
    MetadataBuild {
        source: stackable_operator::builder::meta::Error,
    },
    #[snafu(display("failed to build Labels"))]
    LabelBuild {
        source: stackable_operator::kvp::LabelError,
    },
}

/// The rolegroup [`Service`] is a headless service that allows direct access to the instances of a certain rolegroup
///
/// This is mostly useful for internal communication between peers, or for clients that perform client-side load balancing.
pub fn build_rolegroup_headless_service(
    hive: &v1alpha1::HiveCluster,
    resolved_product_image: &ResolvedProductImage,
    rolegroup: &RoleGroupRef<v1alpha1::HiveCluster>,
) -> Result<Service, Error> {
    let headless_service = Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(hive)
            // TODO: Use method on RoleGroupRef once op-rs is released
            .name(rolegroup_headless_service_name(rolegroup))
            .ownerreference_from_resource(hive, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(build_recommended_labels(
                hive,
                &resolved_product_image.app_version_label_value,
                &rolegroup.role,
                &rolegroup.role_group,
            ))
            .context(MetadataBuildSnafu)?
            .build(),
        spec: Some(ServiceSpec {
            // Internal communication does not need to be exposed
            type_: Some("ClusterIP".to_string()),
            cluster_ip: Some("None".to_string()),
            // Expecting same ports as on listener service, just as a headless, internal service
            ports: Some(service_ports()),
            selector: Some(
                Labels::role_group_selector(hive, APP_NAME, &rolegroup.role, &rolegroup.role_group)
                    .context(LabelBuildSnafu)?
                    .into(),
            ),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    };
    Ok(headless_service)
}

/// The rolegroup metrics [`Service`] is a service that exposes metrics and a prometheus scraping label
pub fn build_rolegroup_metrics_service(
    hive: &v1alpha1::HiveCluster,
    resolved_product_image: &ResolvedProductImage,
    rolegroup: &RoleGroupRef<v1alpha1::HiveCluster>,
) -> Result<Service, Error> {
    let metrics_service = Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(hive)
            // TODO: Use method on RoleGroupRef once op-rs is released
            .name(rolegroup_metrics_service_name(rolegroup))
            .ownerreference_from_resource(hive, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(build_recommended_labels(
                hive,
                &resolved_product_image.app_version_label_value,
                &rolegroup.role,
                &rolegroup.role_group,
            ))
            .context(MetadataBuildSnafu)?
            .with_label(Label::try_from(("prometheus.io/scrape", "true")).context(LabelBuildSnafu)?)
            .build(),
        spec: Some(ServiceSpec {
            // Internal communication does not need to be exposed
            type_: Some("ClusterIP".to_string()),
            cluster_ip: Some("None".to_string()),
            ports: Some(metrics_ports()),
            selector: Some(
                Labels::role_group_selector(hive, APP_NAME, &rolegroup.role, &rolegroup.role_group)
                    .context(LabelBuildSnafu)?
                    .into(),
            ),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    };
    Ok(metrics_service)
}

/// Headless service for cluster internal purposes only.
// TODO: Move to operator-rs
pub fn rolegroup_headless_service_name(rolegroup: &RoleGroupRef<v1alpha1::HiveCluster>) -> String {
    format!("{name}-headless", name = rolegroup.object_name())
}

/// Headless metrics service exposes Prometheus endpoint only
// TODO: Move to operator-rs
pub fn rolegroup_metrics_service_name(rolegroup: &RoleGroupRef<v1alpha1::HiveCluster>) -> String {
    format!("{name}-metrics", name = rolegroup.object_name())
}

fn metrics_ports() -> Vec<ServicePort> {
    vec![ServicePort {
        name: Some(METRICS_PORT_NAME.to_string()),
        port: METRICS_PORT.into(),
        protocol: Some("TCP".to_string()),
        ..ServicePort::default()
    }]
}

fn service_ports() -> Vec<ServicePort> {
    vec![ServicePort {
        name: Some(HIVE_PORT_NAME.to_string()),
        port: HIVE_PORT.into(),
        protocol: Some("TCP".to_string()),
        ..ServicePort::default()
    }]
}

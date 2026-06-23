use stackable_operator::{
    k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec},
    v2::builder::service::{Scheme, Scraping, prometheus_annotations, prometheus_labels},
};

use crate::{
    controller::{RoleGroupName, ValidatedCluster},
    crd::{HIVE_PORT, HIVE_PORT_NAME, METRICS_PORT, METRICS_PORT_NAME},
};

/// The rolegroup [`Service`] is a headless service that allows direct access to the instances of a certain rolegroup
///
/// This is mostly useful for internal communication between peers, or for clients that perform client-side load balancing.
pub fn build_rolegroup_headless_service(
    cluster: &ValidatedCluster,
    role_group_name: &RoleGroupName,
) -> Service {
    Service {
        metadata: cluster
            .object_meta(
                cluster
                    .resource_names(role_group_name)
                    .headless_service_name()
                    .to_string(),
                role_group_name,
            )
            .build(),
        spec: Some(ServiceSpec {
            // Internal communication does not need to be exposed
            type_: Some("ClusterIP".to_string()),
            cluster_ip: Some("None".to_string()),
            // Expecting same ports as on listener service, just as a headless, internal service
            ports: Some(service_ports()),
            selector: Some(cluster.role_group_selector(role_group_name).into()),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    }
}

/// The rolegroup metrics [`Service`] is a service that exposes metrics and a prometheus scraping label
pub fn build_rolegroup_metrics_service(
    cluster: &ValidatedCluster,
    role_group_name: &RoleGroupName,
) -> Service {
    Service {
        metadata: cluster
            .object_meta(
                cluster
                    .resource_names(role_group_name)
                    .metrics_service_name()
                    .to_string(),
                role_group_name,
            )
            .with_labels(prometheus_labels(&Scraping::Enabled))
            .with_annotations(prometheus_annotations(
                &Scraping::Enabled,
                &Scheme::Http,
                "/metrics",
                &METRICS_PORT,
            ))
            .build(),
        spec: Some(ServiceSpec {
            // Internal communication does not need to be exposed
            type_: Some("ClusterIP".to_string()),
            cluster_ip: Some("None".to_string()),
            ports: Some(metrics_ports()),
            selector: Some(cluster.role_group_selector(role_group_name).into()),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    }
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

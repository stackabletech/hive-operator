use stackable_operator::{
    k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec},
    v2::builder::service::{Scheme, Scraping, prometheus_annotations, prometheus_labels},
};

use crate::{
    controller::{
        RoleGroupName, ValidatedCluster,
        build::{object_meta, recommended_labels_for_role_group_resources, role_group_selector},
    },
    crd::{HIVE_PORT, HIVE_PORT_NAME, HiveRole, METRICS_PORT, METRICS_PORT_NAME},
};

/// The rolegroup [`Service`] is a headless service that allows direct access to the instances of a certain rolegroup
///
/// This is mostly useful for internal communication between peers, or for clients that perform client-side load balancing.
pub fn build_rolegroup_headless_service(
    cluster: &ValidatedCluster,
    hive_role: &HiveRole,
    role_group_name: &RoleGroupName,
) -> Service {
    Service {
        metadata: object_meta(
            cluster,
            cluster
                .role_group_resource_names(role_group_name)
                .headless_service_name()
                .to_string(),
            recommended_labels_for_role_group_resources(cluster, hive_role, role_group_name),
        )
        .build(),
        spec: Some(ServiceSpec {
            // Internal communication does not need to be exposed
            type_: Some("ClusterIP".to_string()),
            cluster_ip: Some("None".to_string()),
            // Expecting same ports as on listener service, just as a headless, internal service
            ports: Some(service_ports()),
            selector: Some(role_group_selector(cluster, hive_role, role_group_name).into()),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    }
}

/// The rolegroup metrics [`Service`] is a service that exposes metrics and a prometheus scraping label
pub fn build_rolegroup_metrics_service(
    cluster: &ValidatedCluster,
    hive_role: &HiveRole,
    role_group_name: &RoleGroupName,
) -> Service {
    Service {
        metadata: object_meta(
            cluster,
            cluster
                .role_group_resource_names(role_group_name)
                .metrics_service_name()
                .to_string(),
            recommended_labels_for_role_group_resources(cluster, hive_role, role_group_name),
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
            selector: Some(role_group_selector(cluster, hive_role, role_group_name).into()),
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

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use serde_json::json;

    use super::*;
    use crate::controller::test_support::{
        DERBY_YAML, app_version_label, minimal_hive, validated_cluster,
    };

    /// Every metrics Service must carry the Prometheus scrape label and the
    /// `prometheus.io/path|port|scheme|scrape` annotations, or Prometheus stops discovering the
    /// endpoints.
    #[test]
    fn test_rolegroup_metrics_service() {
        let hive = minimal_hive(DERBY_YAML);
        let cluster = validated_cluster(&hive);
        let role_group_name = RoleGroupName::from_str("default").expect("valid role group name");

        let service =
            build_rolegroup_metrics_service(&cluster, &HiveRole::MetaStore, &role_group_name);

        assert_eq!(
            json!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {
                    "annotations": {
                        "prometheus.io/path": "/metrics",
                        "prometheus.io/port": "9084",
                        "prometheus.io/scheme": "http",
                        "prometheus.io/scrape": "true"
                    },
                    "labels": {
                        "app.kubernetes.io/component": "metastore",
                        "app.kubernetes.io/instance": "simple-hive",
                        "app.kubernetes.io/managed-by": "hive.stackable.tech_hivecluster",
                        "app.kubernetes.io/name": "hive",
                        "app.kubernetes.io/role-group": "default",
                        "app.kubernetes.io/version": app_version_label("4.0.0"),
                        "prometheus.io/scrape": "true",
                        "stackable.tech/vendor": "Stackable"
                    },
                    "name": "simple-hive-metastore-default-metrics",
                    "namespace": "default",
                    "ownerReferences": [
                        {
                            "apiVersion": "hive.stackable.tech/v1alpha1",
                            "controller": true,
                            "kind": "HiveCluster",
                            "name": "simple-hive",
                            "uid": "12345678-1234-1234-1234-123456789012"
                        }
                    ]
                },
                "spec": {
                    "clusterIP": "None",
                    "ports": [
                        {
                            "name": "metrics",
                            "port": 9084,
                            "protocol": "TCP"
                        }
                    ],
                    "publishNotReadyAddresses": true,
                    "selector": {
                        "app.kubernetes.io/component": "metastore",
                        "app.kubernetes.io/instance": "simple-hive",
                        "app.kubernetes.io/name": "hive",
                        "app.kubernetes.io/role-group": "default"
                    },
                    "type": "ClusterIP"
                }
            }),
            serde_json::to_value(service).expect("must be serializable")
        );
    }
}

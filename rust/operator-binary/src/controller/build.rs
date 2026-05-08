use std::marker::PhantomData;

use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    commons::rbac::build_rbac_resources,
    constants::RESTART_CONTROLLER_ENABLED_LABEL,
    crd::listener::v1alpha1::{Listener, ListenerPort, ListenerSpec},
    k8s_openapi::{
        DeepMerge,
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                Affinity, ConfigMapVolumeSource, Container as K8sContainer, ContainerPort,
                EmptyDirVolumeSource, PodSpec, PodTemplateSpec, Probe, Service, ServicePort,
                ServiceSpec, TCPSocketAction, Volume, VolumeMount,
            },
        },
        apimachinery::pkg::{
            api::resource::Quantity, apis::meta::v1::LabelSelector, util::intstr::IntOrString,
        },
    },
    kvp::{Annotations, Labels},
    product_logging,
    role_utils::RoleGroupRef,
};

use super::{
    HIVE_CONTROLLER_NAME, KubernetesResources, Prepared, ValidatedHiveCluster,
    ValidatedRoleGroupConfig, validate::MAX_HIVE_LOG_FILES_SIZE,
};
use crate::{
    OPERATOR_NAME,
    crd::{
        APP_NAME, Container, HIVE_PORT, HIVE_PORT_NAME, HIVE_SITE_XML, HiveRole,
        JVM_SECURITY_PROPERTIES_FILE, METRICS_PORT, METRICS_PORT_NAME, STACKABLE_CONFIG_DIR,
        STACKABLE_CONFIG_DIR_NAME, STACKABLE_CONFIG_MOUNT_DIR, STACKABLE_CONFIG_MOUNT_DIR_NAME,
        STACKABLE_LOG_CONFIG_MOUNT_DIR, STACKABLE_LOG_CONFIG_MOUNT_DIR_NAME, STACKABLE_LOG_DIR,
        STACKABLE_LOG_DIR_NAME,
    },
    framework,
    framework::builder::meta::ownerreference_from_resource,
    listener::{LISTENER_VOLUME_DIR, LISTENER_VOLUME_NAME},
    product_logging::extend_role_group_config_map,
};

pub fn build(validated: &ValidatedHiveCluster) -> KubernetesResources<Prepared> {
    let mut stateful_sets = Vec::new();
    let mut config_maps = Vec::new();
    let mut services = Vec::new();
    let mut service_accounts = Vec::new();
    let mut role_bindings = Vec::new();
    let mut pod_disruption_budgets = Vec::new();
    let mut listeners = Vec::new();

    // RBAC
    let required_labels = {
        use crate::framework::types::operator::*;
        framework::kvp::label::role_selector(
            validated,
            &ProductName::from_str_unsafe(APP_NAME),
            &RoleName::from_str_unsafe(&HiveRole::MetaStore.to_string()),
        )
    };
    let (rbac_sa, rbac_rolebinding) = build_rbac_resources(validated, APP_NAME, required_labels)
        .expect("RBAC resources should be created");
    service_accounts.push(rbac_sa);
    role_bindings.push(rbac_rolebinding);

    for (role, rg_configs) in &validated.role_groups {
        for (rolegroup_name, rg_config) in rg_configs {
            let pod_data = &validated.precomputed_pod_data[role][rolegroup_name];
            let rolegroup_ref = validated.rolegroup_ref(role, rolegroup_name);

            // ConfigMap
            config_maps.push(build_rolegroup_config_map(
                validated,
                &rolegroup_ref,
                rg_config,
            ));

            // Services
            services.push(build_rolegroup_headless_service(validated, &rolegroup_ref));
            services.push(build_rolegroup_metrics_service(validated, &rolegroup_ref));

            // StatefulSet
            stateful_sets.push(build_rolegroup_statefulset(
                validated,
                &rolegroup_ref,
                rg_config,
                pod_data,
            ));
        }

        // PDB
        if let Some(role_config) = validated.role_configs.get(role) {
            if role_config.pdb_enabled {
                let max_unavailable = role_config.pdb_max_unavailable;
                let pdb_resource = {
                    use crate::framework::types::operator::*;
                    framework::builder::pdb::pod_disruption_budget_builder_with_role(
                        validated,
                        &ProductName::from_str_unsafe(APP_NAME),
                        &RoleName::from_str_unsafe(&role.to_string()),
                        &OperatorName::from_str_unsafe(OPERATOR_NAME),
                        &ControllerName::from_str_unsafe(HIVE_CONTROLLER_NAME),
                    )
                    .with_max_unavailable(max_unavailable)
                    .build()
                };
                pod_disruption_budgets.push(pdb_resource);
            }

            // Listener
            listeners.push(build_role_listener(validated, role, role_config));
        }
    }

    KubernetesResources {
        stateful_sets,
        config_maps,
        services,
        service_accounts,
        role_bindings,
        pod_disruption_budgets,
        listeners,
        discovery_hash: None,
        _status: PhantomData,
    }
}

fn recommended_labels(validated: &ValidatedHiveCluster, role: &str, role_group: &str) -> Labels {
    use crate::framework::types::operator::*;
    framework::kvp::label::recommended_labels(
        validated,
        &ProductName::from_str_unsafe(APP_NAME),
        &ProductVersion::from_str_unsafe(&validated.image.app_version_label_value.to_string()),
        &OperatorName::from_str_unsafe(OPERATOR_NAME),
        &ControllerName::from_str_unsafe(HIVE_CONTROLLER_NAME),
        &RoleName::from_str_unsafe(role),
        &RoleGroupName::from_str_unsafe(role_group),
    )
}

fn role_group_selector_labels(
    validated: &ValidatedHiveCluster,
    role: &str,
    role_group: &str,
) -> Labels {
    use crate::framework::types::operator::*;
    framework::kvp::label::role_group_selector(
        validated,
        &ProductName::from_str_unsafe(APP_NAME),
        &RoleName::from_str_unsafe(role),
        &RoleGroupName::from_str_unsafe(role_group),
    )
}

fn build_rolegroup_config_map(
    validated: &ValidatedHiveCluster,
    rolegroup: &RoleGroupRef<ValidatedHiveCluster>,
    rg_config: &ValidatedRoleGroupConfig,
) -> stackable_operator::k8s_openapi::api::core::v1::ConfigMap {
    let labels = recommended_labels(validated, &rolegroup.role, &rolegroup.role_group);

    let metadata = ObjectMetaBuilder::new()
        .name(rolegroup.object_name())
        .namespace(&validated.namespace)
        .ownerreference(ownerreference_from_resource(validated, None, Some(true)))
        .with_labels(labels)
        .build();

    let mut cm_builder = ConfigMapBuilder::new();
    cm_builder.metadata(metadata);
    cm_builder.add_data(HIVE_SITE_XML, &rg_config.hive_site_xml_content);
    cm_builder.add_data(
        JVM_SECURITY_PROPERTIES_FILE,
        &rg_config.jvm_security_properties_content,
    );

    // Kerberos core-site.xml (pre-computed during validation)
    if let Some(ref core_site_xml) = rg_config.core_site_xml_content {
        cm_builder.add_data(crate::crd::CORE_SITE_XML, core_site_xml);
    }

    // Logging config
    let logging = build_logging_for_config_map(rg_config);
    extend_role_group_config_map(rolegroup, &logging, &mut cm_builder)
        .expect("Logging configuration should be valid because it was validated earlier");

    cm_builder
        .build()
        .expect("ConfigMap should be created because all required fields are set")
}

fn build_rolegroup_headless_service(
    validated: &ValidatedHiveCluster,
    rolegroup: &RoleGroupRef<ValidatedHiveCluster>,
) -> Service {
    let labels = recommended_labels(validated, &rolegroup.role, &rolegroup.role_group);
    let selector = role_group_selector_labels(validated, &rolegroup.role, &rolegroup.role_group);

    Service {
        metadata: ObjectMetaBuilder::new()
            .name(rolegroup.rolegroup_headless_service_name())
            .namespace(&validated.namespace)
            .ownerreference(ownerreference_from_resource(validated, None, Some(true)))
            .with_labels(labels)
            .build(),
        spec: Some(ServiceSpec {
            type_: Some("ClusterIP".to_string()),
            cluster_ip: Some("None".to_string()),
            ports: Some(service_ports()),
            selector: Some(selector.into()),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    }
}

fn build_rolegroup_metrics_service(
    validated: &ValidatedHiveCluster,
    rolegroup: &RoleGroupRef<ValidatedHiveCluster>,
) -> Service {
    let labels = recommended_labels(validated, &rolegroup.role, &rolegroup.role_group);
    let selector = role_group_selector_labels(validated, &rolegroup.role, &rolegroup.role_group);

    Service {
        metadata: ObjectMetaBuilder::new()
            .name(rolegroup.rolegroup_metrics_service_name())
            .namespace(&validated.namespace)
            .ownerreference(ownerreference_from_resource(validated, None, Some(true)))
            .with_labels(labels.clone())
            .with_labels(prometheus_labels())
            .with_annotations(prometheus_annotations())
            .build(),
        spec: Some(ServiceSpec {
            type_: Some("ClusterIP".to_string()),
            cluster_ip: Some("None".to_string()),
            ports: Some(metrics_ports()),
            selector: Some(selector.into()),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    }
}

fn build_rolegroup_statefulset(
    validated: &ValidatedHiveCluster,
    rolegroup_ref: &RoleGroupRef<ValidatedHiveCluster>,
    rg_config: &ValidatedRoleGroupConfig,
    pod_data: &super::PrecomputedPodData,
) -> StatefulSet {
    let labels = recommended_labels(validated, &rolegroup_ref.role, &rolegroup_ref.role_group);
    let selector =
        role_group_selector_labels(validated, &rolegroup_ref.role, &rolegroup_ref.role_group);

    // Build main container
    let mut volume_mounts = vec![
        VolumeMount {
            name: STACKABLE_CONFIG_DIR_NAME.to_string(),
            mount_path: STACKABLE_CONFIG_DIR.to_string(),
            ..VolumeMount::default()
        },
        VolumeMount {
            name: STACKABLE_CONFIG_MOUNT_DIR_NAME.to_string(),
            mount_path: STACKABLE_CONFIG_MOUNT_DIR.to_string(),
            ..VolumeMount::default()
        },
        VolumeMount {
            name: STACKABLE_LOG_DIR_NAME.to_string(),
            mount_path: STACKABLE_LOG_DIR.to_string(),
            ..VolumeMount::default()
        },
        VolumeMount {
            name: STACKABLE_LOG_CONFIG_MOUNT_DIR_NAME.to_string(),
            mount_path: STACKABLE_LOG_CONFIG_MOUNT_DIR.to_string(),
            ..VolumeMount::default()
        },
        VolumeMount {
            name: LISTENER_VOLUME_NAME.to_string(),
            mount_path: LISTENER_VOLUME_DIR.to_string(),
            ..VolumeMount::default()
        },
    ];
    volume_mounts.extend(pod_data.hdfs_volume_mounts.clone());
    volume_mounts.extend(pod_data.s3_volume_mounts.clone());
    volume_mounts.extend(pod_data.opa_volume_mounts.clone());
    volume_mounts.extend(pod_data.kerberos_volume_mounts.clone());

    let main_container = K8sContainer {
        name: APP_NAME.to_string(),
        image: Some(validated.image.image.clone()),
        image_pull_policy: Some(validated.image.image_pull_policy.clone()),
        command: Some(vec![
            "/bin/bash".to_string(),
            "-x".to_string(),
            "-euo".to_string(),
            "pipefail".to_string(),
            "-c".to_string(),
        ]),
        args: Some(pod_data.commands.clone()),
        env: Some(pod_data.env_vars.clone()),
        ports: Some(vec![
            ContainerPort {
                name: Some(HIVE_PORT_NAME.to_string()),
                container_port: HIVE_PORT.into(),
                ..ContainerPort::default()
            },
            ContainerPort {
                name: Some(METRICS_PORT_NAME.to_string()),
                container_port: METRICS_PORT.into(),
                ..ContainerPort::default()
            },
        ]),
        volume_mounts: Some(volume_mounts),
        resources: Some(rg_config.resources.clone().into()),
        readiness_probe: Some(Probe {
            initial_delay_seconds: Some(10),
            period_seconds: Some(10),
            failure_threshold: Some(5),
            tcp_socket: Some(TCPSocketAction {
                port: IntOrString::String(HIVE_PORT_NAME.to_string()),
                ..TCPSocketAction::default()
            }),
            ..Probe::default()
        }),
        liveness_probe: Some(Probe {
            initial_delay_seconds: Some(30),
            period_seconds: Some(10),
            tcp_socket: Some(TCPSocketAction {
                port: IntOrString::String(HIVE_PORT_NAME.to_string()),
                ..TCPSocketAction::default()
            }),
            ..Probe::default()
        }),
        ..K8sContainer::default()
    };

    let mut containers = vec![main_container];
    if let Some(ref vector) = pod_data.vector_container {
        containers.push(vector.clone());
    }

    // Build volumes
    let mut volumes = vec![
        Volume {
            name: STACKABLE_CONFIG_DIR_NAME.to_string(),
            empty_dir: Some(EmptyDirVolumeSource {
                medium: None,
                size_limit: Some(Quantity("10Mi".to_string())),
            }),
            ..Volume::default()
        },
        Volume {
            name: STACKABLE_CONFIG_MOUNT_DIR_NAME.to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: rolegroup_ref.object_name(),
                ..Default::default()
            }),
            ..Default::default()
        },
    ];

    // Log dir volume
    volumes.push(Volume {
        name: STACKABLE_LOG_DIR_NAME.to_string(),
        empty_dir: Some(EmptyDirVolumeSource {
            medium: None,
            size_limit: Some(product_logging::framework::calculate_log_volume_size_limit(
                &[MAX_HIVE_LOG_FILES_SIZE],
            )),
        }),
        ..Volume::default()
    });

    // Log config mount volume — custom or from rolegroup configmap
    let log_config_volume = match &rg_config.logging.hive_container {
        super::ValidatedContainerLogConfigChoice::Custom(cm_name) => Volume {
            name: STACKABLE_LOG_CONFIG_MOUNT_DIR_NAME.to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: cm_name.to_string(),
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        },
        _ => Volume {
            name: STACKABLE_LOG_CONFIG_MOUNT_DIR_NAME.to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: rolegroup_ref.object_name(),
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        },
    };
    volumes.push(log_config_volume);

    volumes.extend(pod_data.hdfs_volumes.clone());
    volumes.extend(pod_data.s3_volumes.clone());
    volumes.extend(pod_data.opa_volumes.clone());
    volumes.extend(pod_data.kerberos_volumes.clone());

    let affinity = Affinity {
        pod_affinity: rg_config.affinity.pod_affinity.clone(),
        pod_anti_affinity: rg_config.affinity.pod_anti_affinity.clone(),
        node_affinity: rg_config.affinity.node_affinity.clone(),
    };

    let pod_metadata = ObjectMetaBuilder::new().with_labels(labels.clone()).build();

    let mut pod_template = PodTemplateSpec {
        metadata: Some(pod_metadata),
        spec: Some(PodSpec {
            affinity: Some(affinity),
            containers,
            image_pull_secrets: validated.image.pull_secrets.clone(),
            service_account_name: Some(pod_data.service_account_name.clone()),
            security_context: Some(
                stackable_operator::builder::pod::security::PodSecurityContextBuilder::new()
                    .fs_group(1000)
                    .build(),
            ),
            termination_grace_period_seconds: Some(
                rg_config
                    .graceful_shutdown_timeout
                    .as_secs()
                    .try_into()
                    .unwrap_or(i64::MAX),
            ),
            volumes: Some(volumes),
            ..PodSpec::default()
        }),
    };
    pod_template.merge_from(pod_data.pod_overrides.clone());

    StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name(rolegroup_ref.object_name())
            .namespace(&validated.namespace)
            .ownerreference(ownerreference_from_resource(validated, None, Some(true)))
            .with_labels(labels)
            .with_label(RESTART_CONTROLLER_ENABLED_LABEL.to_owned())
            .build(),
        spec: Some(StatefulSetSpec {
            pod_management_policy: Some("Parallel".to_string()),
            replicas: pod_data.replicas.map(i32::from),
            selector: LabelSelector {
                match_labels: Some(selector.into()),
                ..LabelSelector::default()
            },
            service_name: Some(rolegroup_ref.rolegroup_headless_service_name()),
            template: pod_template,
            volume_claim_templates: Some(vec![pod_data.listener_volume_claim_template.clone()]),
            ..StatefulSetSpec::default()
        }),
        status: None,
    }
}

fn build_role_listener(
    validated: &ValidatedHiveCluster,
    role: &HiveRole,
    role_config: &super::ValidatedRoleConfig,
) -> Listener {
    let labels = recommended_labels(validated, &role.to_string(), "none");

    Listener {
        metadata: ObjectMetaBuilder::new()
            .name(&role_config.listener_name)
            .namespace(&validated.namespace)
            .ownerreference(ownerreference_from_resource(validated, None, Some(true)))
            .with_labels(labels)
            .build(),
        spec: ListenerSpec {
            class_name: Some(role_config.listener_class.clone()),
            ports: Some(listener_ports()),
            ..Default::default()
        },
        status: None,
    }
}

fn service_ports() -> Vec<ServicePort> {
    vec![ServicePort {
        name: Some(HIVE_PORT_NAME.to_string()),
        port: HIVE_PORT.into(),
        protocol: Some("TCP".to_string()),
        ..ServicePort::default()
    }]
}

fn metrics_ports() -> Vec<ServicePort> {
    vec![ServicePort {
        name: Some(METRICS_PORT_NAME.to_string()),
        port: METRICS_PORT.into(),
        protocol: Some("TCP".to_string()),
        ..ServicePort::default()
    }]
}

fn listener_ports() -> Vec<ListenerPort> {
    vec![ListenerPort {
        name: HIVE_PORT_NAME.to_owned(),
        port: HIVE_PORT.into(),
        protocol: Some("TCP".to_owned()),
    }]
}

fn prometheus_labels() -> Labels {
    Labels::try_from([("prometheus.io/scrape", "true")]).expect("should be a valid label")
}

fn prometheus_annotations() -> Annotations {
    Annotations::try_from([
        ("prometheus.io/path".to_owned(), "/metrics".to_owned()),
        ("prometheus.io/port".to_owned(), METRICS_PORT.to_string()),
        ("prometheus.io/scheme".to_owned(), "http".to_owned()),
        ("prometheus.io/scrape".to_owned(), "true".to_owned()),
    ])
    .expect("should be valid annotations")
}

fn build_logging_for_config_map(
    rg_config: &ValidatedRoleGroupConfig,
) -> stackable_operator::product_logging::spec::Logging<Container> {
    let mut logging = stackable_operator::product_logging::spec::Logging {
        enable_vector_agent: rg_config.logging.is_vector_agent_enabled(),
        containers: std::collections::BTreeMap::new(),
    };
    logging.containers.insert(
        Container::Hive,
        rg_config
            .logging
            .hive_container
            .to_raw_container_log_config(),
    );
    if let Some(ref vector) = rg_config.logging.vector_container {
        logging.containers.insert(
            Container::Vector,
            vector.log_config.to_raw_container_log_config(),
        );
    }
    logging
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, str::FromStr};

    use stackable_operator::{
        commons::product_image_selection::ResolvedProductImage,
        k8s_openapi::api::core::v1::{PersistentVolumeClaim, PodTemplateSpec},
        kvp::LabelValue,
    };

    use super::*;
    use crate::{
        controller::{
            PrecomputedPodData, ValidatedHiveCluster, ValidatedLogging, ValidatedRoleConfig,
            ValidatedRoleGroupConfig,
        },
        crd::{HiveRole, MetastoreStorageConfig},
        framework::{
            product_logging::framework::ValidatedContainerLogConfigChoice,
            types::{
                kubernetes::{NamespaceName, Uid},
                operator::ClusterName,
            },
        },
        listener::DEFAULT_LISTENER_CLASS,
    };

    fn test_resolved_product_image() -> ResolvedProductImage {
        ResolvedProductImage {
            product_version: "4.0.1".to_string(),
            app_version_label_value: LabelValue::from_str("4.0.1").unwrap(),
            image: "oci.stackable.tech/sdp/hive:4.0.1-stackable0.0.0-dev".to_string(),
            image_pull_policy: "IfNotPresent".to_string(),
            pull_secrets: None,
        }
    }

    fn test_validated_rg_config() -> ValidatedRoleGroupConfig {
        use stackable_operator::{
            commons::resources::{CpuLimits, MemoryLimits, NoRuntimeLimits, Resources},
            k8s_openapi::apimachinery::pkg::api::resource::Quantity,
        };

        ValidatedRoleGroupConfig {
            resources: Resources {
                cpu: CpuLimits {
                    min: Some(Quantity("250m".to_owned())),
                    max: Some(Quantity("1000m".to_owned())),
                },
                memory: MemoryLimits {
                    limit: Some(Quantity("768Mi".to_owned())),
                    runtime_limits: NoRuntimeLimits {},
                },
                storage: MetastoreStorageConfig {
                    data: stackable_operator::commons::resources::PvcConfig {
                        capacity: Some(Quantity("0Mi".to_owned())),
                        storage_class: None,
                        selectors: None,
                    },
                },
            },
            logging: ValidatedLogging {
                hive_container: ValidatedContainerLogConfigChoice::Automatic(
                    stackable_operator::product_logging::spec::AutomaticContainerLogConfig::default(
                    ),
                ),
                vector_container: None,
            },
            affinity: Default::default(),
            graceful_shutdown_timeout:
                stackable_operator::shared::time::Duration::from_minutes_unchecked(5),
            hive_site_xml_content: "<configuration></configuration>".to_string(),
            jvm_security_properties_content: String::new(),
            core_site_xml_content: None,
        }
    }

    fn test_validated_cluster() -> ValidatedHiveCluster {
        let rg_config = test_validated_rg_config();
        let pod_data = PrecomputedPodData {
            env_vars: vec![],
            commands: vec!["echo test".to_string()],
            kerberos_volumes: vec![],
            kerberos_volume_mounts: vec![],
            s3_volumes: vec![],
            s3_volume_mounts: vec![],
            hdfs_volumes: vec![],
            hdfs_volume_mounts: vec![],
            opa_volumes: vec![],
            opa_volume_mounts: vec![],
            vector_container: None,
            service_account_name: format!("{}-test-hive-serviceaccount", APP_NAME),
            replicas: Some(2),
            pod_overrides: PodTemplateSpec::default(),
            listener_volume_claim_template: PersistentVolumeClaim::default(),
        };

        let mut role_groups = BTreeMap::new();
        let mut rg_configs = BTreeMap::new();
        rg_configs.insert("default".to_string(), rg_config);
        role_groups.insert(HiveRole::MetaStore, rg_configs);

        let mut precomputed = BTreeMap::new();
        let mut pod_data_map = BTreeMap::new();
        pod_data_map.insert("default".to_string(), pod_data);
        precomputed.insert(HiveRole::MetaStore, pod_data_map);

        let mut role_configs = BTreeMap::new();
        role_configs.insert(
            HiveRole::MetaStore,
            ValidatedRoleConfig {
                pdb_enabled: true,
                pdb_max_unavailable: 1,
                listener_class: DEFAULT_LISTENER_CLASS.to_string(),
                listener_name: "test-hive-metastore".to_string(),
            },
        );

        ValidatedHiveCluster::new(
            test_resolved_product_image(),
            ClusterName::from_str_unsafe("test-hive"),
            NamespaceName::from_str_unsafe("default"),
            Uid::from_str_unsafe("c27b3971-ca72-42c1-80a4-abdfc1db0ddd"),
            role_groups,
            precomputed,
            role_configs,
        )
    }

    #[test]
    fn build_produces_expected_resource_counts() {
        let validated = test_validated_cluster();

        let resources = build(&validated);

        assert_eq!(
            resources.service_accounts.len(),
            1,
            "one service account for the role"
        );
        assert_eq!(
            resources.role_bindings.len(),
            1,
            "one role binding for the role"
        );
        assert_eq!(
            resources.config_maps.len(),
            1,
            "one config map per role group"
        );
        assert_eq!(
            resources.services.len(),
            2,
            "headless + metrics service per role group"
        );
        assert_eq!(
            resources.stateful_sets.len(),
            1,
            "one statefulset per role group"
        );
        assert_eq!(
            resources.pod_disruption_budgets.len(),
            1,
            "one PDB per role"
        );
        assert_eq!(resources.listeners.len(), 1, "one listener per role");
    }

    #[test]
    fn build_statefulset_has_correct_name_and_namespace() {
        let validated = test_validated_cluster();

        let resources = build(&validated);
        let sts = &resources.stateful_sets[0];

        assert_eq!(
            sts.metadata.name.as_deref(),
            Some("test-hive-metastore-default")
        );
        assert_eq!(sts.metadata.namespace.as_deref(), Some("default"));
    }

    #[test]
    fn build_statefulset_has_correct_replicas() {
        let validated = test_validated_cluster();

        let resources = build(&validated);
        let sts = &resources.stateful_sets[0];

        assert_eq!(sts.spec.as_ref().unwrap().replicas, Some(2),);
    }

    #[test]
    fn build_headless_service_is_cluster_ip_none() {
        let validated = test_validated_cluster();

        let resources = build(&validated);
        let headless = resources
            .services
            .iter()
            .find(|s| {
                s.metadata
                    .name
                    .as_deref()
                    .is_some_and(|n| !n.contains("metrics"))
            })
            .expect("headless service should exist");

        let spec = headless.spec.as_ref().unwrap();
        assert_eq!(spec.cluster_ip.as_deref(), Some("None"));
        assert_eq!(spec.type_.as_deref(), Some("ClusterIP"));
    }

    #[test]
    fn build_metrics_service_has_prometheus_annotations() {
        let validated = test_validated_cluster();

        let resources = build(&validated);
        let metrics = resources
            .services
            .iter()
            .find(|s| {
                s.metadata
                    .name
                    .as_deref()
                    .is_some_and(|n| n.contains("metrics"))
            })
            .expect("metrics service should exist");

        let annotations = metrics.metadata.annotations.as_ref().unwrap();
        assert_eq!(
            annotations.get("prometheus.io/scrape"),
            Some(&"true".to_string()),
        );
        assert_eq!(
            annotations.get("prometheus.io/port"),
            Some(&METRICS_PORT.to_string()),
        );
    }

    #[test]
    fn build_listener_has_correct_port() {
        let validated = test_validated_cluster();

        let resources = build(&validated);
        let listener = &resources.listeners[0];

        assert_eq!(
            listener.metadata.name.as_deref(),
            Some("test-hive-metastore")
        );
        let ports = listener.spec.ports.as_ref().unwrap();
        assert_eq!(ports.len(), 1);
        assert_eq!(ports[0].name, HIVE_PORT_NAME);
        assert_eq!(ports[0].port, i32::from(HIVE_PORT));
    }

    #[test]
    fn build_statefulset_has_owner_reference() {
        let validated = test_validated_cluster();

        let resources = build(&validated);
        let sts = &resources.stateful_sets[0];

        let owner_refs = sts.metadata.owner_references.as_ref().unwrap();
        assert_eq!(owner_refs.len(), 1);
        assert_eq!(owner_refs[0].name, "test-hive");
        assert_eq!(owner_refs[0].uid, "c27b3971-ca72-42c1-80a4-abdfc1db0ddd");
        assert_eq!(owner_refs[0].controller, Some(true));
    }

    #[test]
    fn build_statefulset_main_container_has_expected_ports() {
        let validated = test_validated_cluster();

        let resources = build(&validated);
        let sts = &resources.stateful_sets[0];

        let containers = &sts
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers;
        assert_eq!(
            containers.len(),
            1,
            "no vector container when vector is disabled"
        );

        let main = &containers[0];
        assert_eq!(main.name, APP_NAME);
        let ports = main.ports.as_ref().unwrap();
        let port_names: Vec<&str> = ports.iter().filter_map(|p| p.name.as_deref()).collect();
        assert!(port_names.contains(&HIVE_PORT_NAME));
        assert!(port_names.contains(&METRICS_PORT_NAME));
    }
}

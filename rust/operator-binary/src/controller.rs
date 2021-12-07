//! Ensures that `Pod`s are configured and running for each [`HiveCluster`]

use crate::{
    discovery::{self, build_discovery_configmaps},
    utils::{apply_owned, apply_status},
    APP_NAME, APP_PORT, METRICS_PORT,
};
use fnv::FnvHasher;
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_hive_crd::{
    DbType, HiveCluster, HiveClusterSpec, HiveClusterStatus, HiveRole, MetaStoreConfig,
    RoleGroupRef, CONFIG_DIR_NAME, HIVE_SITE_XML, LOG_4J_PROPERTIES,
};
use stackable_operator::{
    builder::{ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder, PodBuilder},
    k8s_openapi::{
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMap, ConfigMapVolumeSource, PersistentVolumeClaim, PersistentVolumeClaimSpec,
                ResourceRequirements, Service, ServicePort, ServiceSpec, Volume,
            },
        },
        apimachinery::pkg::{api::resource::Quantity, apis::meta::v1::LabelSelector},
    },
    kube::{
        self,
        api::ObjectMeta,
        runtime::{
            controller::{Context, ReconcilerAction},
            reflector::ObjectRef,
        },
    },
    labels::{role_group_selector_labels, role_selector_labels},
    product_config::{types::PropertyNameKind, ProductConfigManager},
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
};
use std::str::FromStr;
use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    hash::Hasher,
    time::Duration,
};
use tracing::warn;

const FIELD_MANAGER: &str = "hive.stackable.tech/hivecluster";

pub struct Ctx {
    pub kube: kube::Client,
    pub product_config: ProductConfigManager,
}

#[derive(Snafu, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("object {} has no namespace", obj_ref))]
    ObjectHasNoNamespace { obj_ref: ObjectRef<HiveCluster> },
    #[snafu(display("object {} defines no version", obj_ref))]
    ObjectHasNoVersion { obj_ref: ObjectRef<HiveCluster> },
    #[snafu(display("{} has no server role", obj_ref))]
    NoMetaStoreRole { obj_ref: ObjectRef<HiveCluster> },
    #[snafu(display("failed to calculate global service name for {}", obj_ref))]
    GlobalServiceNameNotFound { obj_ref: ObjectRef<HiveCluster> },
    #[snafu(display("failed to calculate service name for role {}", rolegroup))]
    RoleGroupServiceNameNotFound { rolegroup: RoleGroupRef },
    #[snafu(display("failed to apply global Service for {}", hive))]
    ApplyRoleService {
        source: kube::Error,
        hive: ObjectRef<HiveCluster>,
    },
    #[snafu(display("failed to apply Service for {}", rolegroup))]
    ApplyRoleGroupService {
        source: kube::Error,
        rolegroup: RoleGroupRef,
    },
    #[snafu(display("failed to build ConfigMap for {}", rolegroup))]
    BuildRoleGroupConfig {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef,
    },
    #[snafu(display("failed to apply ConfigMap for {}", rolegroup))]
    ApplyRoleGroupConfig {
        source: kube::Error,
        rolegroup: RoleGroupRef,
    },
    #[snafu(display("failed to apply StatefulSet for {}", rolegroup))]
    ApplyRoleGroupStatefulSet {
        source: kube::Error,
        rolegroup: RoleGroupRef,
    },
    #[snafu(display("invalid product config for {}", hive))]
    InvalidProductConfig {
        source: stackable_operator::error::Error,
        hive: ObjectRef<HiveCluster>,
    },
    #[snafu(display("failed to serialize zoo.cfg for {}", rolegroup))]
    SerializeZooCfg {
        source: stackable_operator::product_config::writer::PropertiesWriterError,
        rolegroup: RoleGroupRef,
    },
    #[snafu(display("object {} is missing metadata to build owner reference", hive))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
        hive: ObjectRef<HiveCluster>,
    },
    #[snafu(display("failed to build discovery ConfigMap for {}", hive))]
    BuildDiscoveryConfig {
        source: discovery::Error,
        hive: ObjectRef<HiveCluster>,
    },
    #[snafu(display("failed to apply discovery ConfigMap for {}", hive))]
    ApplyDiscoveryConfig {
        source: kube::Error,
        hive: ObjectRef<HiveCluster>,
    },
    #[snafu(display("failed to update status of {}", hive))]
    ApplyStatus {
        source: kube::Error,
        hive: ObjectRef<HiveCluster>,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub async fn reconcile_hive(hive: HiveCluster, ctx: Context<Ctx>) -> Result<ReconcilerAction> {
    tracing::info!("Starting reconcile");
    let hive_ref = ObjectRef::from_obj(&hive);
    let kube = ctx.get_ref().kube.clone();
    let hive_version = hive_version(&hive)?;

    let validated_config = validate_all_roles_and_groups_config(
        hive_version,
        &transform_all_roles_to_config(
            &hive,
            [(
                HiveRole::MetaStore.to_string(),
                (
                    vec![
                        PropertyNameKind::Env,
                        PropertyNameKind::Cli,
                        PropertyNameKind::File(HIVE_SITE_XML.to_string()),
                    ],
                    hive.spec
                        .metastore
                        .clone()
                        .with_context(|| NoMetaStoreRole {
                            obj_ref: hive_ref.clone(),
                        })?,
                ),
            )]
            .into(),
        ),
        &ctx.get_ref().product_config,
        false,
        false,
    )
    .with_context(|| InvalidProductConfig {
        hive: hive_ref.clone(),
    })?;

    let metastore_config = validated_config
        .get(&HiveRole::MetaStore.to_string())
        .map(Cow::Borrowed)
        .unwrap_or_default();

    let metastore_role_service =
        apply_owned(&kube, FIELD_MANAGER, &build_metastore_role_service(&hive)?)
            .await
            .with_context(|| ApplyRoleService {
                hive: hive_ref.clone(),
            })?;
    for (rolegroup_name, rolegroup_config) in metastore_config.iter() {
        let rolegroup = hive.metastore_rolegroup_ref(rolegroup_name);

        apply_owned(
            &kube,
            FIELD_MANAGER,
            &build_metastore_rolegroup_service(&rolegroup, &hive, &metastore_config)?,
        )
        .await
        .with_context(|| ApplyRoleGroupService {
            rolegroup: rolegroup.clone(),
        })?;
        apply_owned(
            &kube,
            FIELD_MANAGER,
            &build_metastore_rolegroup_config_map(&rolegroup, &hive, rolegroup_config)?,
        )
        .await
        .with_context(|| ApplyRoleGroupConfig {
            rolegroup: rolegroup.clone(),
        })?;
        apply_owned(
            &kube,
            FIELD_MANAGER,
            &build_metastore_rolegroup_statefulset(&rolegroup, &hive, rolegroup_config)?,
        )
        .await
        .with_context(|| ApplyRoleGroupStatefulSet {
            rolegroup: rolegroup.clone(),
        })?;
    }

    // std's SipHasher is deprecated, and DefaultHasher is unstable across Rust releases.
    // We don't /need/ stability, but it's still nice to avoid spurious changes where possible.
    let mut discovery_hash = FnvHasher::with_key(0);
    for discovery_cm in
        build_discovery_configmaps(&kube, &hive, &hive, &metastore_role_service, None)
            .await
            .with_context(|| BuildDiscoveryConfig {
                hive: hive_ref.clone(),
            })?
    {
        let discovery_cm = apply_owned(&kube, FIELD_MANAGER, &discovery_cm)
            .await
            .with_context(|| ApplyDiscoveryConfig {
                hive: hive_ref.clone(),
            })?;
        if let Some(generation) = discovery_cm.metadata.resource_version {
            discovery_hash.write(generation.as_bytes())
        }
    }

    let status = HiveClusterStatus {
        // Serialize as a string to discourage users from trying to parse the value,
        // and to keep things flexible if we end up changing the hasher at some point.
        discovery_hash: Some(discovery_hash.finish().to_string()),
    };
    apply_status(&kube, FIELD_MANAGER, &{
        let mut hive_with_status = HiveCluster::new(&hive_ref.name, HiveClusterSpec::default());
        hive_with_status.metadata.namespace = hive.metadata.namespace.clone();
        hive_with_status.status = Some(status);
        hive_with_status
    })
    .await
    .context(ApplyStatus {
        hive: hive_ref.clone(),
    })?;

    Ok(ReconcilerAction {
        requeue_after: None,
    })
}

/// The server-role service is the primary endpoint that should be used by clients that do not perform internal load balancing,
/// including targets outside of the cluster.
///
/// Note that you should generally *not* hard-code clients to use these services; instead, create a [`HiveZnode`](`stackable_zookeeper_crd::HiveZnode`)
/// and use the connection string that it gives you.
pub fn build_metastore_role_service(hive: &HiveCluster) -> Result<Service> {
    let role_name = HiveRole::MetaStore.to_string();

    let role_svc_name =
        hive.metastore_role_service_name()
            .with_context(|| GlobalServiceNameNotFound {
                obj_ref: ObjectRef::from_obj(hive),
            })?;
    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(hive)
            .name(&role_svc_name)
            .ownerreference_from_resource(hive, None, Some(true))
            .with_context(|| ObjectMissingMetadataForOwnerRef {
                hive: ObjectRef::from_obj(hive),
            })?
            .with_recommended_labels(hive, APP_NAME, hive_version(hive)?, &role_name, "global")
            .build(),
        spec: Some(ServiceSpec {
            ports: Some(vec![ServicePort {
                name: Some("hive".to_string()),
                port: APP_PORT.into(),
                protocol: Some("TCP".to_string()),
                ..ServicePort::default()
            }]),
            selector: Some(role_selector_labels(hive, APP_NAME, &role_name)),
            type_: Some("NodePort".to_string()),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

/// The rolegroup [`ConfigMap`] configures the rolegroup based on the configuration given by the administrator
fn build_metastore_rolegroup_config_map(
    rolegroup: &RoleGroupRef,
    hive: &HiveCluster,
    metastore_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Result<ConfigMap> {
    let mut hive_site_data = String::new();
    let log4j_data = include_str!("../../../deploy/external/log4j.properties");

    for (property_name_kind, config) in metastore_config {
        match property_name_kind {
            PropertyNameKind::File(file_name) if file_name == HIVE_SITE_XML => {
                let mut data = BTreeMap::new();

                for (property_name, property_value) in config {
                    data.insert(property_name.to_string(), Some(property_value.to_string()));
                }

                // TODO: make configurable
                data.insert(
                    "hive.metastore.warehouse.dir".to_string(),
                    Some("/stackable/warehouse".to_string()),
                );

                hive_site_data =
                    stackable_operator::product_config::writer::to_hadoop_xml(data.iter());
            }
            _ => {}
        }
    }

    ConfigMapBuilder::new()
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(hive)
                .name(rolegroup.object_name())
                .ownerreference_from_resource(hive, None, Some(true))
                .with_context(|| ObjectMissingMetadataForOwnerRef {
                    hive: ObjectRef::from_obj(hive),
                })?
                .with_recommended_labels(
                    hive,
                    APP_NAME,
                    hive_version(hive)?,
                    &rolegroup.role,
                    &rolegroup.role_group,
                )
                .build(),
        )
        .add_data(HIVE_SITE_XML, hive_site_data)
        .add_data(LOG_4J_PROPERTIES, log4j_data)
        .build()
        .with_context(|| BuildRoleGroupConfig {
            rolegroup: rolegroup.clone(),
        })
}

/// The rolegroup [`Service`] is a headless service that allows direct access to the instances of a certain rolegroup
///
/// This is mostly useful for internal communication between peers, or for clients that perform client-side load balancing.
fn build_metastore_rolegroup_service(
    rolegroup: &RoleGroupRef,
    hive: &HiveCluster,
    config: &HashMap<String, HashMap<PropertyNameKind, BTreeMap<String, String>>>,
) -> Result<Service> {
    let metastore_port = config.get(&rolegroup.role_group).and_then(|group| {
        group
            .get(&PropertyNameKind::File(HIVE_SITE_XML.to_string()))
            .and_then(|config| config.get(MetaStoreConfig::METASTORE_PORT_PROPERTY))
            .map(|p| p.parse::<i32>().unwrap_or(APP_PORT.into()))
    });

    let metrics_port = config.get(&rolegroup.role_group).and_then(|group| {
        group
            .get(&PropertyNameKind::Env)
            .and_then(|config| config.get(MetaStoreConfig::METRICS_PORT_PROPERTY))
            .map(|p| p.parse::<i32>().unwrap_or(METRICS_PORT.into()))
    });

    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(hive)
            .name(&rolegroup.object_name())
            .ownerreference_from_resource(hive, None, Some(true))
            .with_context(|| ObjectMissingMetadataForOwnerRef {
                hive: ObjectRef::from_obj(hive),
            })?
            .with_recommended_labels(
                hive,
                APP_NAME,
                hive_version(hive)?,
                &rolegroup.role,
                &rolegroup.role_group,
            )
            .build(),
        spec: Some(ServiceSpec {
            cluster_ip: Some("None".to_string()),
            ports: Some(vec![
                ServicePort {
                    name: Some("hive".to_string()),
                    port: metastore_port.unwrap_or(APP_PORT.into()),
                    protocol: Some("TCP".to_string()),
                    ..ServicePort::default()
                },
                ServicePort {
                    name: Some("metrics".to_string()),
                    port: metrics_port.unwrap_or(METRICS_PORT.into()),
                    protocol: Some("TCP".to_string()),
                    ..ServicePort::default()
                },
            ]),
            selector: Some(role_group_selector_labels(
                hive,
                APP_NAME,
                &rolegroup.role,
                &rolegroup.role_group,
            )),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

/// The rolegroup [`StatefulSet`] runs the rolegroup, as configured by the administrator.
///
/// The [`Pod`](`stackable_operator::k8s_openapi::api::core::v1::Pod`)s are accessible through the
/// corresponding [`Service`] (from [`build_rolegroup_service`]).
fn build_metastore_rolegroup_statefulset(
    rolegroup_ref: &RoleGroupRef,
    hive: &HiveCluster,
    metastore_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Result<StatefulSet> {
    let mut db_type: Option<DbType> = None;
    let mut container_builder = ContainerBuilder::new(APP_NAME);

    for (property_name_kind, config) in metastore_config {
        match property_name_kind {
            PropertyNameKind::File(file_name) if file_name == HIVE_SITE_XML => {
                let metastore_port = config
                    .get(MetaStoreConfig::METASTORE_PORT_PROPERTY)
                    .and_then(|p| i32::from_str(p).ok())
                    .unwrap_or_else(|| APP_PORT.into());

                container_builder.add_container_port("hive", metastore_port);
            }
            PropertyNameKind::Env => {
                for (property_name, property_value) in config {
                    if property_name.is_empty() {
                        warn!("Received empty property_name for ENV... skipping");
                        continue;
                    }
                    // if a metrics port is provided (for now by user, it is not required in
                    // product config to be able to not configure any monitoring / metrics)
                    if property_name == MetaStoreConfig::METRICS_PORT_PROPERTY {
                        container_builder.add_container_port(
                            "metrics",
                            property_value.parse().unwrap_or(METRICS_PORT.into()),
                        );
                        container_builder.add_env_var(
                            "HADOOP_OPTS".to_string(),
                            format!("-javaagent:/stackable/jmx/jmx_prometheus_javaagent-0.16.1.jar={}:/stackable/jmx/jmx_hive_config.yaml", property_value)
                        );
                        continue;
                    }

                    container_builder.add_env_var(property_name, property_value);
                }
            }
            PropertyNameKind::Cli => {
                for (property_name, property_value) in config {
                    if property_name == MetaStoreConfig::DB_TYPE_CLI {
                        // TODO: remove unwrap
                        db_type = Some(DbType::from_str(property_value).unwrap());
                    }
                }
            }
            _ => {}
        }
    }

    let rolegroup = hive
        .spec
        .metastore
        .as_ref()
        .with_context(|| NoMetaStoreRole {
            obj_ref: ObjectRef::from_obj(hive),
        })?
        .role_groups
        .get(&rolegroup_ref.role_group);

    let hive_version = hive_version(hive)?;
    let image = format!(
        "docker.stackable.tech/stackable/hive:{}-stackable0",
        hive_version
    );

    let container_hive = container_builder
        .image(image)
        .command(HiveRole::MetaStore.get_command(true, &db_type.unwrap_or_default().to_string()))
        .add_volume_mount("conf", CONFIG_DIR_NAME)
        .build();

    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(hive)
            .name(&rolegroup_ref.object_name())
            .ownerreference_from_resource(hive, None, Some(true))
            .with_context(|| ObjectMissingMetadataForOwnerRef {
                hive: ObjectRef::from_obj(hive),
            })?
            .with_recommended_labels(
                hive,
                APP_NAME,
                hive_version,
                &rolegroup_ref.role,
                &rolegroup_ref.role_group,
            )
            .build(),
        spec: Some(StatefulSetSpec {
            pod_management_policy: Some("Parallel".to_string()),
            replicas: if hive.spec.stopped.unwrap_or(false) {
                Some(0)
            } else {
                rolegroup.and_then(|rg| rg.replicas).map(i32::from)
            },
            selector: LabelSelector {
                match_labels: Some(role_group_selector_labels(
                    hive,
                    APP_NAME,
                    &rolegroup_ref.role,
                    &rolegroup_ref.role_group,
                )),
                ..LabelSelector::default()
            },
            service_name: rolegroup_ref.object_name(),
            template: PodBuilder::new()
                .metadata_builder(|m| {
                    m.with_recommended_labels(
                        hive,
                        APP_NAME,
                        hive_version,
                        &rolegroup_ref.role,
                        &rolegroup_ref.role_group,
                    )
                })
                .add_container(container_hive)
                .add_volume(Volume {
                    name: "conf".to_string(),
                    config_map: Some(ConfigMapVolumeSource {
                        name: Some(rolegroup_ref.object_name()),
                        ..ConfigMapVolumeSource::default()
                    }),
                    ..Volume::default()
                })
                .build_template(),
            volume_claim_templates: Some(vec![PersistentVolumeClaim {
                metadata: ObjectMeta {
                    name: Some("data".to_string()),
                    ..ObjectMeta::default()
                },
                spec: Some(PersistentVolumeClaimSpec {
                    access_modes: Some(vec!["ReadWriteOnce".to_string()]),
                    resources: Some(ResourceRequirements {
                        requests: Some({
                            let mut map = BTreeMap::new();
                            map.insert("storage".to_string(), Quantity("1Gi".to_string()));
                            map
                        }),
                        ..ResourceRequirements::default()
                    }),
                    ..PersistentVolumeClaimSpec::default()
                }),
                ..PersistentVolumeClaim::default()
            }]),
            ..StatefulSetSpec::default()
        }),
        status: None,
    })
}

pub fn hive_version(hive: &HiveCluster) -> Result<&str> {
    hive.spec
        .version
        .as_deref()
        .with_context(|| ObjectHasNoVersion {
            obj_ref: ObjectRef::from_obj(hive),
        })
}

pub fn error_policy(_error: &Error, _ctx: Context<Ctx>) -> ReconcilerAction {
    ReconcilerAction {
        requeue_after: Some(Duration::from_secs(5)),
    }
}

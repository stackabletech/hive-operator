//! Ensures that `Pod`s are configured and running for each [`HiveCluster`]
use crate::discovery;

use fnv::FnvHasher;
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_hive_crd::{
    DbType, HiveCluster, HiveClusterStatus, HiveRole, MetaStoreConfig, APP_NAME, CONFIG_DIR_NAME,
    HIVE_PORT, HIVE_PORT_NAME, HIVE_SITE_XML, LOG_4J_PROPERTIES, METRICS_PORT, METRICS_PORT_NAME,
};
use stackable_operator::k8s_openapi::api::core::v1::{Probe, TCPSocketAction};
use stackable_operator::k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use stackable_operator::role_utils::RoleGroupRef;
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
        api::ObjectMeta,
        runtime::controller::{Context, ReconcilerAction},
    },
    labels::{role_group_selector_labels, role_selector_labels},
    product_config::{types::PropertyNameKind, ProductConfigManager},
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
};
use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    hash::Hasher,
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use tracing::warn;

const FIELD_MANAGER_SCOPE: &str = "hivecluster";

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub product_config: ProductConfigManager,
}

#[derive(Snafu, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("object defines no version"))]
    ObjectHasNoVersion,
    #[snafu(display("object defines no metastore role"))]
    NoMetaStoreRole,
    #[snafu(display("failed to calculate global service name"))]
    GlobalServiceNameNotFound,
    #[snafu(display("failed to calculate service name for role {rolegroup}"))]
    RoleGroupServiceNameNotFound {
        rolegroup: RoleGroupRef<HiveCluster>,
    },
    #[snafu(display("failed to apply global Service"))]
    ApplyRoleService {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to apply Service for {rolegroup}"))]
    ApplyRoleGroupService {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<HiveCluster>,
    },
    #[snafu(display("failed to build ConfigMap for {rolegroup}"))]
    BuildRoleGroupConfig {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<HiveCluster>,
    },
    #[snafu(display("failed to apply ConfigMap for {rolegroup}"))]
    ApplyRoleGroupConfig {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<HiveCluster>,
    },
    #[snafu(display("failed to apply StatefulSet for {rolegroup}"))]
    ApplyRoleGroupStatefulSet {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<HiveCluster>,
    },
    #[snafu(display("failed to generate product config"))]
    GenerateProductConfig {
        source: stackable_operator::product_config_utils::ConfigError,
    },
    #[snafu(display("invalid product config"))]
    InvalidProductConfig {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to build discovery ConfigMap"))]
    BuildDiscoveryConfig { source: discovery::Error },
    #[snafu(display("failed to apply discovery ConfigMap"))]
    ApplyDiscoveryConfig {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to parse db type {db_type}"))]
    InvalidDbType {
        source: strum::ParseError,
        db_type: String,
    },
    #[snafu(display("failed to write discovery config map"))]
    InvalidDiscovery { source: discovery::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub async fn reconcile_hive(hive: Arc<HiveCluster>, ctx: Context<Ctx>) -> Result<ReconcilerAction> {
    tracing::info!("Starting reconcile");
    let client = &ctx.get_ref().client;
    let hive_version = hive_version(&hive)?;

    let validated_config = validate_all_roles_and_groups_config(
        hive_version,
        &transform_all_roles_to_config(
            &*hive,
            [(
                HiveRole::MetaStore.to_string(),
                (
                    vec![
                        PropertyNameKind::Env,
                        PropertyNameKind::Cli,
                        PropertyNameKind::File(HIVE_SITE_XML.to_string()),
                    ],
                    hive.spec.metastore.clone().context(NoMetaStoreRoleSnafu)?,
                ),
            )]
            .into(),
        )
        .context(GenerateProductConfigSnafu)?,
        &ctx.get_ref().product_config,
        false,
        false,
    )
    .context(InvalidProductConfigSnafu)?;

    let metastore_config = validated_config
        .get(&HiveRole::MetaStore.to_string())
        .map(Cow::Borrowed)
        .unwrap_or_default();

    let metastore_role_service = build_metastore_role_service(&hive)?;
    let metastore_role_service = client
        .apply_patch(
            FIELD_MANAGER_SCOPE,
            &metastore_role_service,
            &metastore_role_service,
        )
        .await
        .context(ApplyRoleServiceSnafu)?;

    for (rolegroup_name, rolegroup_config) in metastore_config.iter() {
        let rolegroup = hive.metastore_rolegroup_ref(rolegroup_name);

        let rg_service = build_rolegroup_service(&hive, &rolegroup)?;
        let rg_configmap =
            build_metastore_rolegroup_config_map(&hive, &rolegroup, rolegroup_config)?;
        let rg_statefulset =
            build_metastore_rolegroup_statefulset(&hive, &rolegroup, rolegroup_config)?;

        client
            .apply_patch(FIELD_MANAGER_SCOPE, &rg_service, &rg_service)
            .await
            .with_context(|_| ApplyRoleGroupServiceSnafu {
                rolegroup: rolegroup.clone(),
            })?;
        client
            .apply_patch(FIELD_MANAGER_SCOPE, &rg_configmap, &rg_configmap)
            .await
            .with_context(|_| ApplyRoleGroupConfigSnafu {
                rolegroup: rolegroup.clone(),
            })?;
        client
            .apply_patch(FIELD_MANAGER_SCOPE, &rg_statefulset, &rg_statefulset)
            .await
            .with_context(|_| ApplyRoleGroupStatefulSetSnafu {
                rolegroup: rolegroup.clone(),
            })?;
    }

    // std's SipHasher is deprecated, and DefaultHasher is unstable across Rust releases.
    // We don't /need/ stability, but it's still nice to avoid spurious changes where possible.
    let mut discovery_hash = FnvHasher::with_key(0);
    for discovery_cm in
        discovery::build_discovery_configmaps(client, &*hive, &*hive, &metastore_role_service, None)
            .await
            .context(BuildDiscoveryConfigSnafu)?
    {
        let discovery_cm = client
            .apply_patch(FIELD_MANAGER_SCOPE, &discovery_cm, &discovery_cm)
            .await
            .context(ApplyDiscoveryConfigSnafu)?;
        if let Some(generation) = discovery_cm.metadata.resource_version {
            discovery_hash.write(generation.as_bytes())
        }
    }

    let status = HiveClusterStatus {
        // Serialize as a string to discourage users from trying to parse the value,
        // and to keep things flexible if we end up changing the hasher at some point.
        discovery_hash: Some(discovery_hash.finish().to_string()),
    };
    client
        .apply_patch_status(FIELD_MANAGER_SCOPE, &*hive, &status)
        .await
        .context(ApplyStatusSnafu)?;

    Ok(ReconcilerAction {
        requeue_after: None,
    })
}

/// The server-role service is the primary endpoint that should be used by clients that do not
/// perform internal load balancing including targets outside of the cluster.
pub fn build_metastore_role_service(hive: &HiveCluster) -> Result<Service> {
    let role_name = HiveRole::MetaStore.to_string();

    let role_svc_name = hive
        .metastore_role_service_name()
        .context(GlobalServiceNameNotFoundSnafu)?;
    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(hive)
            .name(&role_svc_name)
            .ownerreference_from_resource(hive, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(hive, APP_NAME, hive_version(hive)?, &role_name, "global")
            .build(),
        spec: Some(ServiceSpec {
            ports: Some(service_ports()),
            selector: Some(role_selector_labels(hive, APP_NAME, &role_name)),
            type_: Some("NodePort".to_string()),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

/// The rolegroup [`ConfigMap`] configures the rolegroup based on the configuration given by the administrator
fn build_metastore_rolegroup_config_map(
    hive: &HiveCluster,
    rolegroup: &RoleGroupRef<HiveCluster>,
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

                data.insert(
                    MetaStoreConfig::METASTORE_WAREHOUSE_DIR.to_string(),
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
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
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
        .with_context(|_| BuildRoleGroupConfigSnafu {
            rolegroup: rolegroup.clone(),
        })
}

/// The rolegroup [`Service`] is a headless service that allows direct access to the instances of a certain rolegroup
///
/// This is mostly useful for internal communication between peers, or for clients that perform client-side load balancing.
fn build_rolegroup_service(
    hive: &HiveCluster,
    rolegroup: &RoleGroupRef<HiveCluster>,
) -> Result<Service> {
    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(hive)
            .name(&rolegroup.object_name())
            .ownerreference_from_resource(hive, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(
                hive,
                APP_NAME,
                hive_version(hive)?,
                &rolegroup.role,
                &rolegroup.role_group,
            )
            .with_label("prometheus.io/scrape", "true")
            .build(),
        spec: Some(ServiceSpec {
            cluster_ip: Some("None".to_string()),
            ports: Some(service_ports()),
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
/// corresponding [`Service`] (from [`build_metastore_rolegroup_service`]).
fn build_metastore_rolegroup_statefulset(
    hive: &HiveCluster,
    rolegroup_ref: &RoleGroupRef<HiveCluster>,
    metastore_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Result<StatefulSet> {
    let mut db_type: Option<DbType> = None;
    let mut container_builder = ContainerBuilder::new(APP_NAME);

    for (property_name_kind, config) in metastore_config {
        match property_name_kind {
            PropertyNameKind::Env => {
                for (property_name, property_value) in config {
                    if property_name.is_empty() {
                        warn!("Received empty property_name for ENV... skipping");
                        continue;
                    }
                    container_builder.add_env_var(property_name, property_value);
                }
            }
            PropertyNameKind::Cli => {
                for (property_name, property_value) in config {
                    if property_name == MetaStoreConfig::DB_TYPE_CLI {
                        db_type = Some(DbType::from_str(property_value).with_context(|_| {
                            InvalidDbTypeSnafu {
                                db_type: property_value.to_string(),
                            }
                        })?);
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
        .context(NoMetaStoreRoleSnafu)?
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
        .add_container_port(HIVE_PORT_NAME, HIVE_PORT.into())
        .add_container_port(METRICS_PORT_NAME, METRICS_PORT.into())
        .readiness_probe(Probe {
            initial_delay_seconds: Some(10),
            period_seconds: Some(10),
            failure_threshold: Some(5),
            tcp_socket: Some(TCPSocketAction {
                port: IntOrString::String(HIVE_PORT_NAME.to_string()),
                ..TCPSocketAction::default()
            }),
            ..Probe::default()
        })
        .liveness_probe(Probe {
            initial_delay_seconds: Some(30),
            period_seconds: Some(10),
            tcp_socket: Some(TCPSocketAction {
                port: IntOrString::String(HIVE_PORT_NAME.to_string()),
                ..TCPSocketAction::default()
            }),
            ..Probe::default()
        })
        .build();

    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(hive)
            .name(&rolegroup_ref.object_name())
            .ownerreference_from_resource(hive, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
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
        .context(ObjectHasNoVersionSnafu)
}

pub fn error_policy(_error: &Error, _ctx: Context<Ctx>) -> ReconcilerAction {
    ReconcilerAction {
        requeue_after: Some(Duration::from_secs(5)),
    }
}

pub fn service_ports() -> Vec<ServicePort> {
    vec![
        ServicePort {
            name: Some(HIVE_PORT_NAME.to_string()),
            port: HIVE_PORT.into(),
            protocol: Some("TCP".to_string()),
            ..ServicePort::default()
        },
        ServicePort {
            name: Some(METRICS_PORT_NAME.to_string()),
            port: METRICS_PORT.into(),
            protocol: Some("TCP".to_string()),
            ..ServicePort::default()
        },
    ]
}

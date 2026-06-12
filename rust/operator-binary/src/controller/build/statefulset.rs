//! Builds the rolegroup [`StatefulSet`] that runs the Hive metastore.

use std::str::FromStr;

use indoc::formatdoc;
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{
        meta::ObjectMetaBuilder,
        pod::{
            PodBuilder,
            security::PodSecurityContextBuilder,
            volume::{SecretOperatorVolumeSourceBuilder, VolumeBuilder},
        },
    },
    commons::secret_class::SecretClassVolumeProvisionParts,
    constants::RESTART_CONTROLLER_ENABLED_LABEL,
    k8s_openapi::{
        DeepMerge,
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMapVolumeSource, EmptyDirVolumeSource, Probe, TCPSocketAction, Volume,
            },
        },
        apimachinery::pkg::{
            api::resource::Quantity, apis::meta::v1::LabelSelector, util::intstr::IntOrString,
        },
    },
    product_logging,
    product_logging::framework::{
        create_vector_shutdown_file_command, remove_vector_shutdown_file_command,
    },
    utils::COMMON_BASH_TRAP_FUNCTIONS,
    v2::{
        builder::{
            meta::ownerreference_from_resource,
            pod::{
                container::{EnvVarSet, new_container_builder},
                volume::{ListenerReference, listener_operator_volume_source_builder_build_pvc},
            },
        },
        product_logging::framework::{
            STACKABLE_LOG_DIR, ValidatedContainerLogConfigChoice, vector_container,
        },
        types::kubernetes::{ContainerName, ListenerName, PersistentVolumeClaimName, VolumeName},
        types::operator::ProductVersion,
    },
};

use crate::{
    controller::{
        HiveRoleGroupConfig, RoleGroupName, ValidatedCluster,
        build::{
            command::build_container_command_args,
            graceful_shutdown::add_graceful_shutdown_config,
            jvm::{construct_hadoop_heapsize_env, construct_non_heap_jvm_args},
            kerberos::{add_kerberos_pod_config, kerberos_container_start_commands},
            opa::OPA_TLS_VOLUME_NAME,
            properties::product_logging::MAX_HIVE_LOG_FILES_SIZE,
        },
    },
    crd::{
        HIVE_PORT, HIVE_PORT_NAME, HiveRole, METRICS_PORT, METRICS_PORT_NAME, STACKABLE_CONFIG_DIR,
        STACKABLE_CONFIG_DIR_NAME, STACKABLE_CONFIG_MOUNT_DIR, STACKABLE_CONFIG_MOUNT_DIR_NAME,
        STACKABLE_LOG_CONFIG_MOUNT_DIR, STACKABLE_LOG_CONFIG_MOUNT_DIR_NAME,
        STACKABLE_LOG_DIR_NAME, v1alpha1,
    },
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("failed to configure S3 connection"))]
    ConfigureS3Connection {
        source: stackable_operator::crd::s3::v1alpha1::ConnectionError,
    },

    #[snafu(display(
        "Hive does not support skipping the verification of the tls enabled S3 server"
    ))]
    S3TlsNoVerificationNotSupported,

    #[snafu(display("failed to configure graceful shutdown"))]
    GracefulShutdown {
        source: crate::controller::build::graceful_shutdown::Error,
    },

    #[snafu(display("failed to add kerberos config"))]
    AddKerberosConfig {
        source: crate::controller::build::kerberos::Error,
    },

    #[snafu(display("failed to add needed volume"))]
    AddVolume {
        source: stackable_operator::builder::pod::Error,
    },

    #[snafu(display("failed to add needed volumeMount"))]
    AddVolumeMount {
        source: stackable_operator::builder::pod::container::Error,
    },

    #[snafu(display("failed to construct JVM arguments"))]
    ConstructJvmArguments {
        source: crate::controller::build::jvm::Error,
    },

    #[snafu(display("failed to build TLS certificate SecretClass Volume"))]
    TlsCertSecretClassVolumeBuild {
        source: stackable_operator::builder::pod::volume::SecretOperatorVolumeSourceBuilderError,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

stackable_operator::constant!(VECTOR_CONTAINER_NAME: ContainerName = "vector");

// The main Hive container's name. Reuses the `APP_NAME` value (`hive`) so the produced
// Container `name` is unchanged.
stackable_operator::constant!(HIVE_CONTAINER_NAME: ContainerName = "hive");

// Typed name for the listener-operator PersistentVolumeClaim. Reuses the existing
// `LISTENER_VOLUME_NAME` string value (`listener`) so the produced PVC name is unchanged.
stackable_operator::constant!(LISTENER_PVC_NAME: PersistentVolumeClaimName = "listener");

// Typed `VolumeName`s for the Vector container's log-config and log volumes. These reuse the
// existing volume-name string values (`config-mount`/`log`) so the produced volume mounts are
// unchanged.
stackable_operator::constant!(VECTOR_LOG_CONFIG_VOLUME_NAME: VolumeName = "config-mount");
stackable_operator::constant!(VECTOR_LOG_VOLUME_NAME: VolumeName = "log");

// Listener volumes (consumed by the listener-operator PVC and its volume mount below).
pub const LISTENER_VOLUME_NAME: &str = "listener";
pub const LISTENER_VOLUME_DIR: &str = "/stackable/listener";

/// The rolegroup [`StatefulSet`] runs the rolegroup, as configured by the administrator.
///
/// The [`Pod`](`stackable_operator::k8s_openapi::api::core::v1::Pod`)s are accessible through the
/// corresponding [`Service`](`stackable_operator::k8s_openapi::api::core::v1::Service`) (via [`build_rolegroup_headless_service`](super::service::build_rolegroup_headless_service) and metrics from [`build_rolegroup_metrics_service`](super::service::build_rolegroup_metrics_service)).
pub(crate) fn build_metastore_rolegroup_statefulset(
    hive: &v1alpha1::HiveCluster,
    hive_role: &HiveRole,
    cluster: &ValidatedCluster,
    role_group_name: &RoleGroupName,
    rg: &HiveRoleGroupConfig,
    sa_name: &str,
) -> Result<StatefulSet, Error> {
    let resource_names = cluster.resource_names(role_group_name);
    let resolved_product_image = &cluster.image;
    let database_connection_details = &cluster.cluster_config.metadata_database_connection_details;
    let s3_connection = cluster.cluster_config.s3_connection_spec.as_ref();
    let merged_config = &rg.config;
    let hive_opa_config = cluster.cluster_config.hive_opa_config.as_ref();

    let mut container_builder = new_container_builder(&HIVE_CONTAINER_NAME);

    container_builder
        .add_env_var(
            "HADOOP_HEAPSIZE",
            construct_hadoop_heapsize_env(merged_config).context(ConstructJvmArgumentsSnafu)?,
        )
        .add_env_var("HADOOP_OPTS", construct_non_heap_jvm_args(hive, rg))
        .add_env_var(
            "CONTAINERDEBUG_LOG_DIRECTORY",
            format!("{STACKABLE_LOG_DIR}/containerdebug"),
        );
    database_connection_details.add_to_container(&mut container_builder);

    // Environment variable overrides (highest precedence), merged from role and role group.
    // Names are validated during cluster validation, so they can be applied directly here.
    container_builder.add_env_vars(rg.env_overrides.clone());

    let mut pod_builder = PodBuilder::new();

    if let Some(hdfs) = &hive.spec.cluster_config.hdfs {
        pod_builder
            .add_volume(
                VolumeBuilder::new("hdfs-discovery")
                    .with_config_map(&hdfs.config_map)
                    .build(),
            )
            .context(AddVolumeSnafu)?;
        container_builder
            .add_volume_mount("hdfs-discovery", "/stackable/mount/hdfs-config")
            .context(AddVolumeMountSnafu)?;
    }

    if let Some(s3) = s3_connection {
        s3.add_volumes_and_mounts(&mut pod_builder, vec![&mut container_builder])
            .context(ConfigureS3ConnectionSnafu)?;

        if s3.tls.uses_tls() && !s3.tls.uses_tls_verification() {
            S3TlsNoVerificationNotSupportedSnafu.fail()?;
        }
    }

    // Add OPA TLS certs if configured
    if let Some((tls_secret_class, tls_mount_path)) =
        hive_opa_config.as_ref().and_then(|opa_config| {
            opa_config
                .tls_secret_class
                .as_ref()
                .zip(opa_config.tls_ca_cert_mount_path())
        })
    {
        container_builder
            .add_volume_mount(OPA_TLS_VOLUME_NAME, &tls_mount_path)
            .context(AddVolumeMountSnafu)?;

        let opa_tls_volume = VolumeBuilder::new(OPA_TLS_VOLUME_NAME)
            .ephemeral(
                SecretOperatorVolumeSourceBuilder::new(
                    tls_secret_class,
                    // We only need the public CA certificate to verify the OPA server.
                    SecretClassVolumeProvisionParts::Public,
                )
                .build()
                .context(TlsCertSecretClassVolumeBuildSnafu)?,
            )
            .build();

        pod_builder
            .add_volume(opa_tls_volume)
            .context(AddVolumeSnafu)?;
    }

    let db_type = hive.spec.cluster_config.metadata_database.as_hive_db_type();
    let start_command = if resolved_product_image.product_version.starts_with("3.") {
        // The schematool version in 3.1.x does *not* support the `-initOrUpgradeSchema` flag yet, so we can not use that.
        // As we *only* support HMS 3.1.x (or newer) since SDP release 23.11, we can safely assume we are always coming
        // from an existing 3.1.x installation. There is no need to upgrade the schema, we can just check if the schema
        // is already there and create it if it isn't.
        // The script `bin/start-metastore` is buggy (e.g. around version upgrades), but it's sufficient for that job :)
        //
        // TODO: Once we drop support for HMS 3.1.x we can remove this condition and very likely get rid of the
        // "bin/start-metastore" script.
        format!(
            "bin/start-metastore --config {STACKABLE_CONFIG_DIR} --db-type {db_type} --hive-bin-dir bin &"
        )
    } else {
        // schematool versions 4.0.x (and above) support the `-initOrUpgradeSchema`, which is exactly what we need :)
        // Some docs for the schemaTool can be found here: https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=34835119
        formatdoc! {"
            bin/base --config \"{STACKABLE_CONFIG_DIR}\" --service schemaTool -dbType \"{db_type}\" -initOrUpgradeSchema
            bin/base --config \"{STACKABLE_CONFIG_DIR}\" --service metastore &
        "}
    };

    let container_builder = container_builder
        .image_from_product_image(resolved_product_image)
        .command(vec![
            "/bin/bash".to_string(),
            "-x".to_string(),
            "-euo".to_string(),
            "pipefail".to_string(),
            "-c".to_string(),
        ])
        .args(build_container_command_args(
            hive,
            formatdoc! {"
            {kerberos_container_start_commands}

            {COMMON_BASH_TRAP_FUNCTIONS}
            {remove_vector_shutdown_file_command}
            prepare_signal_handlers
            containerdebug --output={STACKABLE_LOG_DIR}/containerdebug-state.json --loop &
            {start_command}
            wait_for_termination $!
            {create_vector_shutdown_file_command}
            ",
                kerberos_container_start_commands = kerberos_container_start_commands(hive),
                remove_vector_shutdown_file_command =
                    remove_vector_shutdown_file_command(STACKABLE_LOG_DIR),
                create_vector_shutdown_file_command =
                    create_vector_shutdown_file_command(STACKABLE_LOG_DIR),
            },
            s3_connection,
            hive_opa_config,
        ))
        .add_volume_mount(STACKABLE_CONFIG_DIR_NAME, STACKABLE_CONFIG_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(STACKABLE_CONFIG_MOUNT_DIR_NAME, STACKABLE_CONFIG_MOUNT_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(STACKABLE_LOG_DIR_NAME, STACKABLE_LOG_DIR)
        .context(AddVolumeMountSnafu)?
        .add_volume_mount(
            STACKABLE_LOG_CONFIG_MOUNT_DIR_NAME,
            STACKABLE_LOG_CONFIG_MOUNT_DIR,
        )
        .context(AddVolumeMountSnafu)?
        .add_container_port(HIVE_PORT_NAME, HIVE_PORT.into())
        .add_container_port(METRICS_PORT_NAME, METRICS_PORT.into())
        .resources(merged_config.resources.clone().into())
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
        });

    // TODO: refactor this when CRD versioning is in place
    // Warn if the capacity field has been set to anything other than 0Mi
    if let Some(Quantity(capacity)) = merged_config.resources.storage.data.capacity.as_ref()
        && capacity != &"0Mi".to_string()
    {
        tracing::warn!(
            "The 'storage' CRD property is set to [{capacity}]. This field is not used and will be removed in a future release."
        );
    }

    let recommended_object_labels = cluster.recommended_labels(role_group_name);
    // Used for PVC templates that cannot be modified once they are deployed. A version value is
    // required, so a constant "none" is used to keep the labels stable across version upgrades.
    let unversioned_recommended_labels = cluster.recommended_labels_for(
        &ProductVersion::from_str("none").expect("'none' is a valid product version"),
        role_group_name,
    );

    let metadata = ObjectMetaBuilder::new()
        .with_labels(recommended_object_labels.clone())
        .build();

    let listener_name = ListenerName::from_str(&cluster.role_listener_name(hive_role))
        .expect("the role listener name is a valid Listener name");
    let pvc = listener_operator_volume_source_builder_build_pvc(
        &ListenerReference::Listener(listener_name),
        &unversioned_recommended_labels,
        &LISTENER_PVC_NAME,
    );

    container_builder
        .add_volume_mount(LISTENER_VOLUME_NAME, LISTENER_VOLUME_DIR)
        .context(AddVolumeMountSnafu)?;

    pod_builder
        .metadata(metadata)
        .image_pull_secrets_from_product_image(resolved_product_image)
        .add_volume(Volume {
            name: STACKABLE_CONFIG_DIR_NAME.to_string(),
            empty_dir: Some(EmptyDirVolumeSource {
                medium: None,
                size_limit: Some(Quantity("10Mi".to_string())),
            }),
            ..Volume::default()
        })
        .context(AddVolumeSnafu)?
        .add_volume(stackable_operator::k8s_openapi::api::core::v1::Volume {
            name: STACKABLE_CONFIG_MOUNT_DIR_NAME.to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: resource_names.role_group_config_map().to_string(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .context(AddVolumeSnafu)?
        .add_empty_dir_volume(
            STACKABLE_LOG_DIR_NAME,
            Some(product_logging::framework::calculate_log_volume_size_limit(
                &[MAX_HIVE_LOG_FILES_SIZE],
            )),
        )
        .context(AddVolumeSnafu)?
        .affinity(&merged_config.affinity)
        .service_account_name(sa_name)
        .security_context(PodSecurityContextBuilder::new().fs_group(1000).build());

    // The Hive container's log config ConfigMap: either the operator-generated one (the rolegroup
    // ConfigMap, which carries the automatic `log4j2.properties`) or a user-provided custom
    // ConfigMap. This branches on the *validated* logging choice (see `ValidatedLogging`).
    let log_config_volume_config_map = match &rg.logging.hive_container {
        ValidatedContainerLogConfigChoice::Custom(config_map_name) => config_map_name.to_string(),
        ValidatedContainerLogConfigChoice::Automatic(_) => {
            resource_names.role_group_config_map().to_string()
        }
    };
    pod_builder
        .add_volume(Volume {
            name: STACKABLE_LOG_CONFIG_MOUNT_DIR_NAME.to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: log_config_volume_config_map,
                ..ConfigMapVolumeSource::default()
            }),
            ..Volume::default()
        })
        .context(AddVolumeSnafu)?;

    add_graceful_shutdown_config(merged_config, &mut pod_builder).context(GracefulShutdownSnafu)?;

    if hive.has_kerberos_enabled() {
        add_kerberos_pod_config(hive, hive_role, container_builder, &mut pod_builder)
            .context(AddKerberosConfigSnafu)?;
    }

    // this is the main container
    pod_builder.add_container(container_builder.build());

    // N.B. the vector container should *follow* the hive container so that the hive one is the
    // default, is started first and can provide any dependencies that vector expects
    if let Some(vector_log_config) = &rg.logging.vector_container {
        pod_builder.add_container(vector_container(
            &VECTOR_CONTAINER_NAME,
            resolved_product_image,
            vector_log_config,
            &resource_names,
            &VECTOR_LOG_CONFIG_VOLUME_NAME,
            &VECTOR_LOG_VOLUME_NAME,
            EnvVarSet::new(),
        ));
    }

    let mut pod_template = pod_builder.build_template();
    // Pod overrides were already merged (role <- role group) during validation.
    pod_template.merge_from(rg.pod_overrides.clone());

    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(cluster)
            .name(resource_names.stateful_set_name().to_string())
            .ownerreference(ownerreference_from_resource(cluster, None, Some(true)))
            .with_labels(recommended_object_labels)
            .with_label(RESTART_CONTROLLER_ENABLED_LABEL.to_owned())
            .build(),
        spec: Some(StatefulSetSpec {
            pod_management_policy: Some("Parallel".to_string()),
            replicas: Some(i32::from(rg.replicas)),
            selector: LabelSelector {
                match_labels: Some(cluster.role_group_selector(role_group_name).into()),
                ..LabelSelector::default()
            },
            service_name: Some(resource_names.headless_service_name().to_string()),
            template: pod_template,
            volume_claim_templates: Some(vec![pvc]),
            ..StatefulSetSpec::default()
        }),
        status: None,
    })
}

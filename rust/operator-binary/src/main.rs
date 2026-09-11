// TODO: Look into how to properly resolve `clippy::large_enum_variant`.
// This will need changes in our and upstream error types.
#![allow(clippy::result_large_err)]

use std::sync::Arc;

use anyhow::anyhow;
use clap::Parser;
use futures::{FutureExt, StreamExt, TryFutureExt};
use stackable_operator::{
    YamlSchema,
    cli::{Command, RunArguments},
    crd::{listener::v1alpha1::Listener, s3},
    eos::EndOfSupportChecker,
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Service, ServiceAccount},
        policy::v1::PodDisruptionBudget,
        rbac::v1::RoleBinding,
    },
    kube::{
        CustomResourceExt, ResourceExt,
        core::DeserializeGuard,
        runtime::{
            Controller,
            events::{Recorder, Reporter},
            reflector::ObjectRef,
            watcher,
        },
    },
    logging::controller::report_controller_reconciled,
    shared::yaml::SerializeOptions,
    telemetry::Tracing,
    utils::signal::{self, SignalWatcher},
};

use crate::{
    controller::HIVE_FULL_CONTROLLER_NAME,
    crd::{HiveCluster, HiveClusterVersion, v1alpha1},
    webhooks::conversion::create_webhook_server,
};

mod controller;
mod crd;
mod webhooks;

mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

const HIVE_OPERATOR_NAME: &str = "hive.stackable.tech";

#[derive(Parser)]
#[clap(about, author)]
struct Opts {
    #[clap(subcommand)]
    cmd: Command,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    match opts.cmd {
        Command::Crd => HiveCluster::merged_crd(HiveClusterVersion::V1Alpha1)?
            .print_yaml_schema(built_info::PKG_VERSION, &SerializeOptions::default())?,
        Command::Run(RunArguments {
            operator_environment,
            watch_namespace,
            maintenance,
            common,
        }) => {
            // NOTE (@NickLarsenNZ): Before stackable-telemetry was used:
            // - The console log level was set by `HIVE_OPERATOR_LOG`, and is now `CONSOLE_LOG` (when using Tracing::pre_configured).
            // - The file log level was set by `HIVE_OPERATOR_LOG`, and is now set via `FILE_LOG` (when using Tracing::pre_configured).
            // - The file log directory was set by `HIVE_OPERATOR_LOG_DIRECTORY`, and is now set by `ROLLING_LOGS_DIR` (or via `--rolling-logs <DIRECTORY>`).
            let _tracing_guard =
                Tracing::pre_configured(built_info::PKG_NAME, common.telemetry).init()?;

            tracing::info!(
                built_info.pkg_version = built_info::PKG_VERSION,
                built_info.git_version = built_info::GIT_VERSION,
                built_info.target = built_info::TARGET,
                built_info.built_time_utc = built_info::BUILT_TIME_UTC,
                built_info.rustc_version = built_info::RUSTC_VERSION,
                "Starting {description}",
                description = built_info::PKG_DESCRIPTION
            );

            // Watches for the SIGTERM signal and sends a signal to all receivers, which gracefully
            // shuts down all concurrent tasks below (EoS checker, controller).
            let sigterm_watcher = SignalWatcher::sigterm()?;

            let eos_checker =
                EndOfSupportChecker::new(built_info::BUILT_TIME_UTC, &maintenance.end_of_support)?
                    .run(sigterm_watcher.handle())
                    .map(anyhow::Ok);

            let client = stackable_operator::client::initialize_operator(
                Some(HIVE_OPERATOR_NAME.to_string()),
                &common.cluster_info,
            )
            .await?;

            let webhook_server = create_webhook_server(
                &operator_environment,
                maintenance.disable_crd_maintenance,
                client.as_kube_client(),
            )
            .await?;

            let webhook_server = webhook_server
                .run(sigterm_watcher.handle())
                .map_err(|err| anyhow!(err).context("failed to run webhook server"));

            let event_recorder = Arc::new(Recorder::new(
                client.as_kube_client(),
                Reporter {
                    controller: HIVE_FULL_CONTROLLER_NAME.to_string(),
                    instance: None,
                },
            ));

            let hive_controller = Controller::new(
                watch_namespace.get_api::<DeserializeGuard<v1alpha1::HiveCluster>>(&client),
                watcher::Config::default(),
            );
            let config_map_store = hive_controller.store();
            let s3_connection_store = hive_controller.store();
            let hive_controller = hive_controller
                .owns(
                    watch_namespace.get_api::<DeserializeGuard<ConfigMap>>(&client),
                    watcher::Config::default(),
                )
                // The role Listener is created by this operator, but its ingress address (from
                // which the discovery ConfigMap is built) is only written asynchronously by the
                // listener-operator -- this watch triggers the reconcile run that builds the
                // discovery ConfigMap once the address is set.
                .owns(
                    watch_namespace.get_api::<DeserializeGuard<Listener>>(&client),
                    watcher::Config::default(),
                )
                .owns(
                    watch_namespace.get_api::<DeserializeGuard<PodDisruptionBudget>>(&client),
                    watcher::Config::default(),
                )
                .owns(
                    watch_namespace.get_api::<DeserializeGuard<RoleBinding>>(&client),
                    watcher::Config::default(),
                )
                .owns(
                    watch_namespace.get_api::<DeserializeGuard<Service>>(&client),
                    watcher::Config::default(),
                )
                .owns(
                    watch_namespace.get_api::<DeserializeGuard<ServiceAccount>>(&client),
                    watcher::Config::default(),
                )
                .owns(
                    watch_namespace.get_api::<DeserializeGuard<StatefulSet>>(&client),
                    watcher::Config::default(),
                )
                .watches(
                    watch_namespace.get_api::<DeserializeGuard<ConfigMap>>(&client),
                    watcher::Config::default(),
                    move |config_map| {
                        config_map_store
                            .state()
                            .into_iter()
                            .filter(move |hive| references_config_map(hive, &config_map))
                            .map(|hive| ObjectRef::from_obj(&*hive))
                    },
                )
                .watches(
                    watch_namespace
                        .get_api::<DeserializeGuard<s3::v1alpha1::S3Connection>>(&client),
                    watcher::Config::default(),
                    move |s3_connection| {
                        s3_connection_store
                            .state()
                            .into_iter()
                            .filter(move |hive| references_s3_connection(hive, &s3_connection))
                            .map(|hive| ObjectRef::from_obj(&*hive))
                    },
                )
                .graceful_shutdown_on(sigterm_watcher.handle())
                .run(
                    controller::reconcile_hive,
                    controller::error_policy,
                    Arc::new(controller::Ctx {
                        client: client.clone(),
                        operator_environment,
                    }),
                )
                // We can let the reporting happen in the background
                .for_each_concurrent(
                    16, // concurrency limit
                    |result| {
                        // The event_recorder needs to be shared across all invocations, so that
                        // events are correctly aggregated
                        let event_recorder = event_recorder.clone();
                        async move {
                            report_controller_reconciled(
                                &event_recorder,
                                HIVE_FULL_CONTROLLER_NAME,
                                &result,
                            )
                            .await;
                        }
                    },
                )
                .map(anyhow::Ok);

            let delayed_hive_controller = async {
                signal::crd_established(&client, v1alpha1::HiveCluster::crd_name(), None).await?;
                hive_controller.await
            };

            futures::try_join!(delayed_hive_controller, eos_checker, webhook_server)?;
        }
    }

    Ok(())
}

fn references_config_map(
    hive: &DeserializeGuard<v1alpha1::HiveCluster>,
    config_map: &DeserializeGuard<ConfigMap>,
) -> bool {
    let Ok(hive) = &hive.0 else {
        return false;
    };

    if hive.namespace() != config_map.namespace() {
        return false;
    }

    match &hive.spec.cluster_config.hdfs {
        Some(hdfs_connection) => hdfs_connection.config_map.as_ref() == config_map.name_any(),
        None => false,
    }
}

fn references_s3_connection(
    hive: &DeserializeGuard<v1alpha1::HiveCluster>,
    s3_connection: &DeserializeGuard<s3::v1alpha1::S3Connection>,
) -> bool {
    let Ok(hive) = &hive.0 else {
        return false;
    };

    if hive.namespace() != s3_connection.namespace() {
        return false;
    }

    match &hive.spec.cluster_config.s3 {
        Some(s3::v1alpha1::InlineConnectionOrReference::Reference(s3_connection_name)) => {
            s3_connection_name == &s3_connection.name_any()
        }
        Some(s3::v1alpha1::InlineConnectionOrReference::Inline(_)) | None => false,
    }
}

#[cfg(test)]
mod tests {
    use indoc::indoc;
    use rstest::rstest;

    use super::*;

    fn hive_cluster(spec_s3: &str) -> DeserializeGuard<v1alpha1::HiveCluster> {
        let hive: DeserializeGuard<v1alpha1::HiveCluster> = serde_yaml::from_str(&format!(
            indoc! {r#"
                apiVersion: hive.stackable.tech/v1alpha1
                kind: HiveCluster
                metadata:
                  name: hive
                  namespace: default
                spec:
                  image:
                    productVersion: 4.2.0
                  clusterConfig:
                    metadataDatabase:
                      derby: {{}}
                    {s3}
                  metastore:
                    roleGroups:
                      default:
                        replicas: 1
            "#},
            s3 = spec_s3
        ))
        .expect("HiveCluster YAML parses");

        assert!(
            hive.0.is_ok(),
            "test fixture spec must deserialize: {:?}",
            hive.0.as_ref().err()
        );

        hive
    }

    fn s3_connection(namespace: &str, name: &str) -> DeserializeGuard<s3::v1alpha1::S3Connection> {
        serde_yaml::from_str(&format!(
            indoc! {r#"
                apiVersion: s3.stackable.tech/v1alpha1
                kind: S3Connection
                metadata:
                  name: {name}
                  namespace: {namespace}
                spec:
                  host: minio
            "#},
            name = name,
            namespace = namespace
        ))
        .expect("S3Connection YAML parses")
    }

    #[rstest]
    #[case::referenced("s3:\n      reference: minio", "default", "minio", true)]
    #[case::other_connection("s3:\n      reference: minio", "default", "other", false)]
    #[case::other_namespace("s3:\n      reference: minio", "elsewhere", "minio", false)]
    #[case::inline("s3:\n      inline:\n        host: minio", "default", "minio", false)]
    #[case::no_s3("", "default", "minio", false)]
    fn references_s3_connection_matches_only_the_referenced_connection(
        #[case] spec_s3: &str,
        #[case] connection_namespace: &str,
        #[case] connection_name: &str,
        #[case] expected: bool,
    ) {
        assert_eq!(
            references_s3_connection(
                &hive_cluster(spec_s3),
                &s3_connection(connection_namespace, connection_name)
            ),
            expected
        );
    }

    #[test]
    fn references_s3_connection_ignores_undeserializable_clusters() {
        let hive = serde_yaml::from_str(indoc! {r#"
            apiVersion: hive.stackable.tech/v1alpha1
            kind: HiveCluster
            metadata:
              name: hive
              namespace: default
            spec: {}
        "#})
        .expect("YAML parses; the invalid spec is captured inside the DeserializeGuard");

        assert!(!references_s3_connection(
            &hive,
            &s3_connection("default", "minio")
        ));
    }
}

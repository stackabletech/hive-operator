// TODO: Look into how to properly resolve `clippy::large_enum_variant`.
// This will need changes in our and upstream error types.
#![allow(clippy::result_large_err)]

mod command;
mod config;
mod controller;
mod crd;
mod discovery;
mod kerberos;
mod listener;
mod operations;
mod product_logging;
mod service;

use std::sync::Arc;

use clap::Parser;
use futures::stream::StreamExt;
use stackable_operator::{
    YamlSchema,
    cli::{Command, RunArguments},
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Service},
    },
    kube::{
        ResourceExt,
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
};

use crate::{
    controller::HIVE_FULL_CONTROLLER_NAME,
    crd::{HiveCluster, HiveClusterVersion, v1alpha1},
};

mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

const OPERATOR_NAME: &str = "hive.stackable.tech";

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
            .print_yaml_schema(built_info::PKG_VERSION, SerializeOptions::default())?,
        Command::Run(RunArguments {
            operator_environment: _,
            watch_namespace,
            product_config,
            maintenance: _,
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

            let product_config = product_config.load(&[
                "deploy/config-spec/properties.yaml",
                "/etc/stackable/hive-operator/config-spec/properties.yaml",
            ])?;

            let client = stackable_operator::client::initialize_operator(
                Some(OPERATOR_NAME.to_string()),
                &common.cluster_info,
            )
            .await?;
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
            hive_controller
                .owns(
                    watch_namespace.get_api::<Service>(&client),
                    watcher::Config::default(),
                )
                .owns(
                    watch_namespace.get_api::<StatefulSet>(&client),
                    watcher::Config::default(),
                )
                .owns(
                    watch_namespace.get_api::<ConfigMap>(&client),
                    watcher::Config::default(),
                )
                .shutdown_on_signal()
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
                .run(
                    controller::reconcile_hive,
                    controller::error_policy,
                    Arc::new(controller::Ctx {
                        client: client.clone(),
                        product_config,
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
                .await;
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

    match &hive.spec.cluster_config.hdfs {
        Some(hdfs_connection) => hdfs_connection.config_map == config_map.name_any(),
        None => false,
    }
}

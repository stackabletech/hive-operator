mod command;
mod controller;
mod discovery;

mod kerberos;
mod operations;
mod product_logging;

use crate::controller::HIVE_CONTROLLER_NAME;

use clap::{crate_description, crate_version, Parser};
use futures::stream::StreamExt;
use stackable_hive_crd::{HiveCluster, APP_NAME};
use stackable_operator::{
    cli::{Command, ProductOperatorRun},
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Service},
    },
    kube::core::DeserializeGuard,
    kube::runtime::{watcher, Controller},
    logging::controller::report_controller_reconciled,
    CustomResourceExt,
};
use std::sync::Arc;

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
        Command::Crd => HiveCluster::print_yaml_schema(built_info::PKG_VERSION)?,
        Command::Run(ProductOperatorRun {
            product_config,
            watch_namespace,
            tracing_target,
        }) => {
            stackable_operator::logging::initialize_logging(
                "HIVE_OPERATOR_LOG",
                APP_NAME,
                tracing_target,
            );
            stackable_operator::utils::print_startup_string(
                crate_description!(),
                crate_version!(),
                built_info::GIT_VERSION,
                built_info::TARGET,
                built_info::BUILT_TIME_UTC,
                built_info::RUSTC_VERSION,
            );

            let product_config = product_config.load(&[
                "deploy/config-spec/properties.yaml",
                "/etc/stackable/hive-operator/config-spec/properties.yaml",
            ])?;

            let client =
                stackable_operator::client::create_client(Some(OPERATOR_NAME.to_string())).await?;

            Controller::new(
                watch_namespace.get_api::<DeserializeGuard<HiveCluster>>(&client),
                watcher::Config::default(),
            )
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
            .run(
                controller::reconcile_hive,
                controller::error_policy,
                Arc::new(controller::Ctx {
                    client: client.clone(),
                    product_config,
                }),
            )
            .map(|res| {
                report_controller_reconciled(
                    &client,
                    &format!("{HIVE_CONTROLLER_NAME}.{OPERATOR_NAME}"),
                    &res,
                );
            })
            .collect::<()>()
            .await;
        }
    }

    Ok(())
}

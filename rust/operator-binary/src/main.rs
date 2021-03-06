mod command;
mod controller;
mod discovery;

use clap::Parser;
use futures::stream::StreamExt;
use stackable_hive_crd::{HiveCluster, APP_NAME};
use stackable_operator::{
    cli::{Command, ProductOperatorRun},
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Service},
    },
    kube::{api::ListParams, runtime::Controller, CustomResourceExt},
    logging::controller::report_controller_reconciled,
};
use std::sync::Arc;

mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

#[derive(Parser)]
#[clap(about = built_info::PKG_DESCRIPTION, author = stackable_operator::cli::AUTHOR)]
struct Opts {
    #[clap(subcommand)]
    cmd: Command,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    match opts.cmd {
        Command::Crd => println!("{}", serde_yaml::to_string(&HiveCluster::crd())?,),
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
                built_info::PKG_DESCRIPTION,
                built_info::PKG_VERSION,
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
                stackable_operator::client::create_client(Some("hive.stackable.tech".to_string()))
                    .await?;

            Controller::new(
                watch_namespace.get_api::<HiveCluster>(&client),
                ListParams::default(),
            )
            .owns(
                watch_namespace.get_api::<Service>(&client),
                ListParams::default(),
            )
            .owns(
                watch_namespace.get_api::<StatefulSet>(&client),
                ListParams::default(),
            )
            .owns(
                watch_namespace.get_api::<ConfigMap>(&client),
                ListParams::default(),
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
                report_controller_reconciled(&client, "hiveclusters.hive.stackable.tech", &res);
            })
            .collect::<()>()
            .await;
        }
    }

    Ok(())
}

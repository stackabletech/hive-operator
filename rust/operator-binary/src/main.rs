mod controller;
mod discovery;

use futures::stream::StreamExt;
use stackable_hive_crd::HiveCluster;
use stackable_operator::cli::Command;
use stackable_operator::k8s_openapi::api::core::v1::Endpoints;
use stackable_operator::{
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Service},
    },
    kube::{
        api::{DynamicObject, ListParams},
        runtime::{
            controller::{Context, ReconcilerAction},
            reflector::ObjectRef,
            Controller,
        },
        CustomResourceExt, Resource,
    },
};
use structopt::StructOpt;

mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

#[derive(StructOpt)]
#[structopt(about = built_info::PKG_DESCRIPTION, author = "Stackable GmbH - info@stackable.de")]
struct Opts {
    #[structopt(subcommand)]
    cmd: Command,
}

/// Erases the concrete types of the controller result, so that we can merge the streams of multiple controllers for different resources.
///
/// In particular, we convert `ObjectRef<K>` into `ObjectRef<DynamicObject>` (which carries `K`'s metadata at runtime instead), and
/// `E` into the trait object `anyhow::Error`.
fn erase_controller_result_type<K: Resource, E: std::error::Error + Send + Sync + 'static>(
    res: Result<(ObjectRef<K>, ReconcilerAction), E>,
) -> anyhow::Result<(ObjectRef<DynamicObject>, ReconcilerAction)> {
    let (obj_ref, action) = res?;
    Ok((obj_ref.erase(), action))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    stackable_operator::logging::initialize_logging("HIVE_OPERATOR_LOG");

    let opts = Opts::from_args();
    match opts.cmd {
        Command::Crd => println!("{}", serde_yaml::to_string(&HiveCluster::crd())?,),
        Command::Run { product_config } => {
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

            let hive_controller_builder =
                Controller::new(client.get_all_api::<HiveCluster>(), ListParams::default());
            let hive_store = hive_controller_builder.store();

            let hive_controller = hive_controller_builder
                .owns(client.get_all_api::<Service>(), ListParams::default())
                .owns(client.get_all_api::<StatefulSet>(), ListParams::default())
                .owns(client.get_all_api::<ConfigMap>(), ListParams::default())
                .watches(
                    client.get_all_api::<Endpoints>(),
                    ListParams::default(),
                    move |endpoints| {
                        hive_store
                            .state()
                            .into_iter()
                            .filter(move |hc| {
                                let _ = &endpoints; // capture endpoints to prevent it from being dropped (2021 warning)
                                hc.metadata.namespace == endpoints.metadata.namespace
                                    && hc.metastore_role_service_name() == endpoints.metadata.name
                            })
                            .map(|hc| ObjectRef::from_obj(&hc))
                    },
                )
                .shutdown_on_signal()
                .run(
                    controller::reconcile_hive,
                    controller::error_policy,
                    Context::new(controller::Ctx {
                        client: client.clone(),
                        product_config,
                    }),
                );

            hive_controller
                .map(erase_controller_result_type)
                .for_each(|res| async {
                    match res {
                        Ok((obj, _)) => tracing::info!(object = %obj, "Reconciled object"),
                        Err(err) => {
                            tracing::error!(
                                error = &*err as &dyn std::error::Error,
                                "Failed to reconcile object",
                            )
                        }
                    }
                })
                .await;
        }
    }

    Ok(())
}

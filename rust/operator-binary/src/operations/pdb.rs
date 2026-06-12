use snafu::{ResultExt, Snafu};
use stackable_operator::{
    client::Client, cluster_resources::ClusterResources, commons::pdb::PdbConfig,
    kube::ResourceExt, v2::builder::pdb::pod_disruption_budget_builder_with_role,
};

use crate::{
    controller::{ValidatedCluster, controller_name, operator_name, product_name},
    crd::HiveRole,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Cannot apply PodDisruptionBudget [{name}]"))]
    ApplyPdb {
        source: stackable_operator::cluster_resources::Error,
        name: String,
    },
}

pub async fn add_pdbs(
    pdb: &PdbConfig,
    cluster: &ValidatedCluster,
    role: &HiveRole,
    client: &Client,
    cluster_resources: &mut ClusterResources<'_>,
) -> Result<(), Error> {
    if !pdb.enabled {
        return Ok(());
    }
    let max_unavailable = pdb.max_unavailable.unwrap_or(match role {
        HiveRole::MetaStore => max_unavailable_metastores(),
    });
    let pdb = pod_disruption_budget_builder_with_role(
        cluster,
        &product_name(),
        &ValidatedCluster::role_name(),
        &operator_name(),
        &controller_name(),
    )
    .with_max_unavailable(max_unavailable)
    .build();
    let pdb_name = pdb.name_any();
    cluster_resources
        .add(client, pdb)
        .await
        .with_context(|_| ApplyPdbSnafu { name: pdb_name })?;

    Ok(())
}

fn max_unavailable_metastores() -> u16 {
    1
}

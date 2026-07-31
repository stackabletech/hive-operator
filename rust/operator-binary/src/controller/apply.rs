//! The apply step in the HiveCluster controller.

use std::marker::PhantomData;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    client::Client,
    cluster_resources::{ClusterResource, ClusterResourceApplyStrategy, ClusterResources},
    deep_merger::ObjectOverrides,
    k8s_openapi::api::core::v1::ConfigMap,
    v2::cluster_resources::cluster_resources_new,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::controller::{
    Applied, KubernetesResources, Prepared, ValidatedCluster, controller_name, operator_name,
    product_name,
};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("failed to apply Kubernetes resource"))]
    ApplyResource {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphanedResources {
        source: stackable_operator::cluster_resources::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Applier for the Kubernetes resource specifications produced by this controller.
///
/// The implementation is not tied to this controller and could theoretically be moved to
/// stackable_operator if [`KubernetesResources`] would contain all possible resource types.
pub struct Applier<'a> {
    client: &'a Client,
    cluster_resources: ClusterResources<'a>,
}

impl<'a> Applier<'a> {
    pub fn new(
        client: &'a Client,
        cluster: &ValidatedCluster,
        apply_strategy: ClusterResourceApplyStrategy,
        object_overrides: &'a ObjectOverrides,
    ) -> Applier<'a> {
        let cluster_resources = cluster_resources_new(
            &product_name(),
            &operator_name(),
            &controller_name(),
            &cluster.name,
            &cluster.namespace,
            &cluster.uid,
            apply_strategy,
            object_overrides,
        );

        Applier {
            client,
            cluster_resources,
        }
    }

    /// Applies the given Kubernetes resources and marks them as applied.
    ///
    /// Resources derived from the applied state (the discovery `ConfigMap`) can be applied
    /// afterwards via [`Self::apply_config_maps`]; [`Self::finish`] must be called once all
    /// resources are applied, so that orphaned resources are deleted exactly once at the end.
    pub async fn apply(
        &mut self,
        resources: KubernetesResources<Prepared>,
    ) -> Result<KubernetesResources<Applied>> {
        // Destructured without `..`, so adding a field to [`KubernetesResources`] fails to
        // compile here instead of silently never being applied.
        let KubernetesResources {
            stateful_sets,
            services,
            listeners,
            config_maps,
            pod_disruption_budgets,
            service_accounts,
            role_bindings,
            status: _,
        } = resources;

        // Apply order is: StatefulSets last (a changed mounted ConfigMap/Secret
        // must exist first, else Pods restart -- commons-operator#111). The ServiceAccount comes
        // first because the Pods reference it at creation time.
        let service_accounts = self.add_resources(service_accounts).await?;
        let role_bindings = self.add_resources(role_bindings).await?;
        let services = self.add_resources(services).await?;
        let listeners = self.add_resources(listeners).await?;
        let config_maps = self.add_resources(config_maps).await?;
        let pod_disruption_budgets = self.add_resources(pod_disruption_budgets).await?;
        let stateful_sets = self.add_resources(stateful_sets).await?;

        Ok(KubernetesResources {
            stateful_sets,
            services,
            listeners,
            config_maps,
            pod_disruption_budgets,
            service_accounts,
            role_bindings,
            status: PhantomData,
        })
    }

    /// Applies `ConfigMap`s that are derived from already-applied resources (the discovery
    /// `ConfigMap`, which needs the applied role Listener's ingress addresses).
    pub async fn apply_config_maps(
        &mut self,
        config_maps: Vec<ConfigMap>,
    ) -> Result<Vec<ConfigMap>> {
        self.add_resources(config_maps).await
    }

    /// Deletes resources from earlier reconcile runs that were not applied in this one.
    ///
    /// Must be called exactly once, after every apply phase: a resource applied after this call
    /// would be treated as an orphan and deleted by the next reconcile run.
    pub async fn finish(self) -> Result<()> {
        self.cluster_resources
            .delete_orphaned_resources(self.client)
            .await
            .context(DeleteOrphanedResourcesSnafu)
    }

    async fn add_resources<T: ClusterResource + Sync>(
        &mut self,
        resources: Vec<T>,
    ) -> Result<Vec<T>> {
        let mut applied_resources = vec![];

        for resource in resources {
            let applied_resource = self
                .cluster_resources
                .add(self.client, resource)
                .await
                .context(ApplyResourceSnafu)?;
            applied_resources.push(applied_resource);
        }

        Ok(applied_resources)
    }
}

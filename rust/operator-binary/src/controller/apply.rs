use std::{hash::Hasher, marker::PhantomData};

use fnv::FnvHasher;
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::{configmap::ConfigMapBuilder, meta::ObjectMetaBuilder},
    cluster_resources::ClusterResources,
    crd::listener::v1alpha1::Listener,
    k8s_openapi::api::core::v1::ConfigMap,
    kvp::{Labels, ObjectLabels},
};
use strum::{EnumDiscriminants, IntoStaticStr};

use super::{Applied, HIVE_CONTROLLER_NAME, KubernetesResources, Prepared, ValidatedHiveCluster};
use crate::{OPERATOR_NAME, crd::APP_NAME, listener::build_listener_connection_string};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("failed to apply service account"))]
    ApplyServiceAccount {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to apply role binding"))]
    ApplyRoleBinding {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to apply ConfigMap for {name}"))]
    ApplyConfigMap {
        source: stackable_operator::cluster_resources::Error,
        name: String,
    },

    #[snafu(display("failed to apply Service for {name}"))]
    ApplyService {
        source: stackable_operator::cluster_resources::Error,
        name: String,
    },

    #[snafu(display("failed to apply StatefulSet for {name}"))]
    ApplyStatefulSet {
        source: stackable_operator::cluster_resources::Error,
        name: String,
    },

    #[snafu(display("failed to apply PodDisruptionBudget"))]
    ApplyPdb {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to apply listener for {role}"))]
    ApplyListener {
        source: stackable_operator::cluster_resources::Error,
        role: String,
    },

    #[snafu(display("failed to build discovery ConfigMap"))]
    BuildDiscoveryConfigMap {
        source: stackable_operator::builder::configmap::Error,
    },

    #[snafu(display("failed to apply discovery ConfigMap"))]
    ApplyDiscoveryConfigMap {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to build discovery ConfigMap metadata"))]
    DiscoveryMetadataBuild {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to configure listener connection string"))]
    ListenerConfiguration { source: crate::listener::Error },

    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphanedResources {
        source: stackable_operator::cluster_resources::Error,
    },
}

pub struct Applier<'a> {
    client: &'a stackable_operator::client::Client,
    cluster_resources: ClusterResources<'a>,
}

impl<'a> Applier<'a> {
    pub fn new(
        client: &'a stackable_operator::client::Client,
        cluster_resources: ClusterResources<'a>,
    ) -> Self {
        Self {
            client,
            cluster_resources,
        }
    }

    pub async fn apply(
        mut self,
        prepared: KubernetesResources<Prepared>,
        validated: &ValidatedHiveCluster,
    ) -> Result<KubernetesResources<Applied>, Error> {
        // RBAC
        for sa in prepared.service_accounts {
            self.cluster_resources
                .add(self.client, sa)
                .await
                .context(ApplyServiceAccountSnafu)?;
        }
        for rb in prepared.role_bindings {
            self.cluster_resources
                .add(self.client, rb)
                .await
                .context(ApplyRoleBindingSnafu)?;
        }

        // ConfigMaps
        for cm in prepared.config_maps {
            let name = cm.metadata.name.clone().unwrap_or_default();
            self.cluster_resources
                .add(self.client, cm)
                .await
                .context(ApplyConfigMapSnafu { name })?;
        }

        // Services
        for svc in prepared.services {
            let name = svc.metadata.name.clone().unwrap_or_default();
            self.cluster_resources
                .add(self.client, svc)
                .await
                .context(ApplyServiceSnafu { name })?;
        }

        // StatefulSets — applied after ConfigMaps to prevent unnecessary Pod restarts
        let mut applied_stateful_sets = Vec::new();
        for ss in prepared.stateful_sets {
            let name = ss.metadata.name.clone().unwrap_or_default();
            let applied = self
                .cluster_resources
                .add(self.client, ss)
                .await
                .context(ApplyStatefulSetSnafu { name })?;
            applied_stateful_sets.push(applied);
        }

        // PDBs
        for pdb in prepared.pod_disruption_budgets {
            self.cluster_resources
                .add(self.client, pdb)
                .await
                .context(ApplyPdbSnafu)?;
        }

        // Listeners + discovery ConfigMaps
        // Discovery ConfigMaps depend on applied Listener status (for connection string building).
        // This is hive-specific: the Listener must be applied first, then we read its status
        // to build the connection string in the discovery ConfigMap.
        let mut discovery_hash = FnvHasher::with_key(0);

        for listener in prepared.listeners {
            let role = listener
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get("app.kubernetes.io/component"))
                .cloned()
                .unwrap_or_else(|| "unknown".to_string());

            let applied_listener: Listener = self
                .cluster_resources
                .add(self.client, listener)
                .await
                .context(ApplyListenerSnafu { role: role.clone() })?;

            for discovery_cm in build_discovery_configmaps(validated, &role, applied_listener)? {
                let applied_cm = self
                    .cluster_resources
                    .add(self.client, discovery_cm)
                    .await
                    .context(ApplyDiscoveryConfigMapSnafu)?;
                if let Some(generation) = applied_cm.metadata.resource_version {
                    discovery_hash.write(generation.as_bytes());
                }
            }
        }

        self.cluster_resources
            .delete_orphaned_resources(self.client)
            .await
            .context(DeleteOrphanedResourcesSnafu)?;

        Ok(KubernetesResources {
            stateful_sets: applied_stateful_sets,
            config_maps: vec![],
            services: vec![],
            service_accounts: vec![],
            role_bindings: vec![],
            pod_disruption_budgets: vec![],
            listeners: vec![],
            discovery_hash: Some(discovery_hash.finish().to_string()),
            _status: PhantomData,
        })
    }
}

fn build_discovery_configmaps(
    validated: &ValidatedHiveCluster,
    role: &str,
    listener: Listener,
) -> Result<Vec<ConfigMap>, Error> {
    let name = validated.name.to_string();

    let recommended_object_labels = ObjectLabels {
        owner: validated,
        app_name: APP_NAME,
        app_version: &validated.image.app_version_label_value,
        operator_name: OPERATOR_NAME,
        controller_name: HIVE_CONTROLLER_NAME,
        role,
        role_group: "discovery",
    };

    let labels = Labels::recommended(&recommended_object_labels)
        .expect("Labels should be created because the cluster name is a valid label value");

    let connection_string = build_listener_connection_string(listener, &role.to_string(), None)
        .context(ListenerConfigurationSnafu)?;

    let mut cm_builder = ConfigMapBuilder::new();
    cm_builder.metadata(
        ObjectMetaBuilder::new()
            .name(&name)
            .namespace(&validated.namespace)
            .ownerreference_from_resource(validated, None, Some(true))
            .context(DiscoveryMetadataBuildSnafu)?
            .with_labels(labels)
            .build(),
    );
    cm_builder.add_data("HIVE", connection_string);

    let discovery_cm = cm_builder.build().context(BuildDiscoveryConfigMapSnafu)?;

    Ok(vec![discovery_cm])
}

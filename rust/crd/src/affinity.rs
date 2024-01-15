use stackable_operator::{
    commons::affinity::{affinity_between_role_pods, StackableAffinityFragment},
    k8s_openapi::api::core::v1::PodAntiAffinity,
};

use crate::{HiveRole, APP_NAME};

pub fn get_affinity(cluster_name: &str, role: &HiveRole) -> StackableAffinityFragment {
    StackableAffinityFragment {
        pod_affinity: None,
        pod_anti_affinity: Some(PodAntiAffinity {
            preferred_during_scheduling_ignored_during_execution: Some(vec![
                affinity_between_role_pods(APP_NAME, cluster_name, &role.to_string(), 70),
            ]),
            required_during_scheduling_ignored_during_execution: None,
        }),
        node_affinity: None,
        node_selector: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rstest::rstest;
    use std::collections::BTreeMap;

    use crate::HiveCluster;
    use stackable_operator::{
        commons::affinity::{StackableAffinity, StackableNodeSelector},
        k8s_openapi::{
            api::core::v1::{
                NodeAffinity, NodeSelector, NodeSelectorRequirement, NodeSelectorTerm,
                PodAffinityTerm, PodAntiAffinity, WeightedPodAffinityTerm,
            },
            apimachinery::pkg::apis::meta::v1::LabelSelector,
        },
    };

    #[rstest]
    #[case(HiveRole::MetaStore)]
    fn test_affinity_defaults(#[case] role: HiveRole) {
        let input = r#"
        apiVersion: hive.stackable.tech/v1alpha1
        kind: HiveCluster
        metadata:
          name: simple-hive
        spec:
          image:
            productVersion: 3.1.3
          clusterConfig:
            database:
              connString: jdbc:derby:;databaseName=/tmp/hive;create=true
              user: APP
              password: mine
              dbType: derby
          metastore:
            roleGroups:
              default:
                replicas: 1
        "#;
        let hive: HiveCluster = serde_yaml::from_str(input).expect("illegal test input");
        let merged_config = hive
            .merged_config(&role, &role.rolegroup_ref(&hive, "default"))
            .unwrap();

        assert_eq!(
            merged_config.affinity,
            StackableAffinity {
                pod_affinity: None,
                pod_anti_affinity: Some(PodAntiAffinity {
                    preferred_during_scheduling_ignored_during_execution: Some(vec![
                        WeightedPodAffinityTerm {
                            pod_affinity_term: PodAffinityTerm {
                                label_selector: Some(LabelSelector {
                                    match_expressions: None,
                                    match_labels: Some(BTreeMap::from([
                                        ("app.kubernetes.io/name".to_string(), "hive".to_string(),),
                                        (
                                            "app.kubernetes.io/instance".to_string(),
                                            "simple-hive".to_string(),
                                        ),
                                        (
                                            "app.kubernetes.io/component".to_string(),
                                            "metastore".to_string(),
                                        )
                                    ]))
                                }),
                                namespace_selector: None,
                                namespaces: None,
                                topology_key: "kubernetes.io/hostname".to_string(),
                            },
                            weight: 70
                        }
                    ]),
                    required_during_scheduling_ignored_during_execution: None,
                }),
                node_affinity: None,
                node_selector: None,
            }
        );
    }
}

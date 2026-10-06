use stackable_operator::{
    commons::{
        affinity::{StackableAffinityFragment, affinity_between_role_pods},
        opa::OpaConfig,
    },
    k8s_openapi::api::core::v1::{PodAffinity, PodAntiAffinity},
};

use crate::crd::{APP_NAME, HiveRole};

pub fn get_affinity(
    cluster_name: &str,
    role: &HiveRole,
    opa_config: Option<&OpaConfig>,
) -> StackableAffinityFragment {
    // With OPA authorization configured, the metastore sends its authorization requests to OPA, so
    // prefer to place it next to the OPA Pods.
    let pod_affinity = opa_config.map(|opa_config| PodAffinity {
        preferred_during_scheduling_ignored_during_execution: Some(vec![
            affinity_between_role_pods(
                "opa",
                &opa_config.config_map_name, // The discovery cm has the same name as the OpaCluster itself
                "server",
                50,
            ),
        ]),
        required_during_scheduling_ignored_during_execution: None,
    });

    StackableAffinityFragment {
        pod_affinity,
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
    use std::collections::BTreeMap;

    use rstest::rstest;
    use stackable_operator::{
        commons::affinity::StackableAffinity,
        k8s_openapi::{
            api::core::v1::{
                PodAffinity, PodAffinityTerm, PodAntiAffinity, WeightedPodAffinityTerm,
            },
            apimachinery::pkg::apis::meta::v1::LabelSelector,
        },
    };

    use super::*;

    #[rstest]
    #[case(HiveRole::MetaStore)]
    fn test_affinity_defaults(#[case] role: HiveRole) {
        let input = r#"
        apiVersion: hive.stackable.tech/v1alpha1
        kind: HiveCluster
        metadata:
          name: simple-hive
          namespace: default
          uid: 12345678-1234-1234-1234-123456789012
        spec:
          image:
            productVersion: 4.2.0
          clusterConfig:
            metadataDatabase:
              derby: {}
            authorization:
              opa:
                configMapName: simple-opa
                package: hive
          metastore:
            roleGroups:
              default:
                replicas: 1
        "#;
        let hive = crate::controller::test_support::minimal_hive(input);
        let validated = crate::controller::test_support::validated_cluster(&hive);
        let merged_config = validated
            .role_group_configs
            .get(&role)
            .and_then(|groups| groups.get(&"default".parse().expect("valid role group name")))
            .expect("role group should exist")
            .config
            .clone();

        assert_eq!(
            merged_config.affinity,
            StackableAffinity {
                pod_affinity: Some(PodAffinity {
                    preferred_during_scheduling_ignored_during_execution: Some(vec![
                        WeightedPodAffinityTerm {
                            pod_affinity_term: PodAffinityTerm {
                                label_selector: Some(LabelSelector {
                                    match_labels: Some(BTreeMap::from([
                                        ("app.kubernetes.io/name".to_string(), "opa".to_string()),
                                        (
                                            "app.kubernetes.io/instance".to_string(),
                                            "simple-opa".to_string(),
                                        ),
                                        (
                                            "app.kubernetes.io/component".to_string(),
                                            "server".to_string(),
                                        ),
                                    ])),
                                    ..LabelSelector::default()
                                }),
                                topology_key: "kubernetes.io/hostname".to_string(),
                                ..PodAffinityTerm::default()
                            },
                            weight: 50,
                        }
                    ]),
                    required_during_scheduling_ignored_during_execution: None,
                }),
                pod_anti_affinity: Some(PodAntiAffinity {
                    preferred_during_scheduling_ignored_during_execution: Some(vec![
                        WeightedPodAffinityTerm {
                            pod_affinity_term: PodAffinityTerm {
                                label_selector: Some(LabelSelector {
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
                                    ])),
                                    ..LabelSelector::default()
                                }),
                                topology_key: "kubernetes.io/hostname".to_string(),
                                ..PodAffinityTerm::default()
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

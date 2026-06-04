//! Renders the logging config files (`log4j2.properties` and the Vector agent
//! config) assembled into the rolegroup `ConfigMap`.

use stackable_operator::{
    memory::BinaryMultiple,
    product_logging::{
        self,
        spec::{
            AutomaticContainerLogConfig, ContainerLogConfig, ContainerLogConfigChoice, Logging,
        },
    },
    role_utils::RoleGroupRef,
};

use crate::{
    controller::MAX_HIVE_LOG_FILES_SIZE,
    crd::{Container, STACKABLE_LOG_DIR, v1alpha1},
};

const CONSOLE_CONVERSION_PATTERN: &str = "%d{ISO8601} %5p [%t] %c{2}: %m%n";
const HIVE_LOG_FILE: &str = "hive.log4j2.xml";

/// Renders `log4j2.properties` for the Hive metastore container.
///
/// Returns `None` when the Hive container does not use the operator's automatic logging
/// configuration (e.g. a custom log ConfigMap is referenced instead), in which case no
/// `log4j2.properties` should be added to the rolegroup `ConfigMap`.
pub fn build_log4j2(logging: &Logging<Container>) -> Option<String> {
    match logging.containers.get(&Container::Hive) {
        Some(ContainerLogConfig {
            choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
        }) => Some(log4j2_config(log_config)),
        _ => None,
    }
}

/// Renders the Vector agent config (`vector.yaml`).
///
/// Returns `None` when the Vector agent is disabled for this role group.
pub fn build_vector_config(
    rolegroup: &RoleGroupRef<v1alpha1::HiveCluster>,
    logging: &Logging<Container>,
) -> Option<String> {
    if !logging.enable_vector_agent {
        return None;
    }

    let vector_log_config = match logging.containers.get(&Container::Vector) {
        Some(ContainerLogConfig {
            choice: Some(ContainerLogConfigChoice::Automatic(log_config)),
        }) => Some(log_config),
        _ => None,
    };

    Some(product_logging::framework::create_vector_config(
        rolegroup,
        vector_log_config,
    ))
}

fn log4j2_config(log_config: &AutomaticContainerLogConfig) -> String {
    product_logging::framework::create_log4j2_config(
        &format!(
            "{STACKABLE_LOG_DIR}/{container}",
            container = Container::Hive
        ),
        HIVE_LOG_FILE,
        MAX_HIVE_LOG_FILES_SIZE
            .scale_to(BinaryMultiple::Mebi)
            .floor()
            .value as u32,
        CONSOLE_CONVERSION_PATTERN,
        log_config,
    )
}

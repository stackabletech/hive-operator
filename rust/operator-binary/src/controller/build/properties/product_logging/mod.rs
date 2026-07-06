//! Renders the logging config files (`log4j2.properties` and the Vector agent
//! config) assembled into the rolegroup `ConfigMap`.

use stackable_operator::{
    memory::{BinaryMultiple, MemoryQuantity},
    product_logging::{self, spec::AutomaticContainerLogConfig},
    v2::product_logging::framework::{STACKABLE_LOG_DIR, ValidatedContainerLogConfigChoice},
};

use crate::crd::Container;

pub(crate) const MAX_HIVE_LOG_FILES_SIZE: MemoryQuantity = MemoryQuantity {
    value: 10.0,
    unit: BinaryMultiple::Mebi,
};

const CONSOLE_CONVERSION_PATTERN: &str = "%d{ISO8601} %5p [%t] %c{2}: %m%n";
const HIVE_LOG_FILE: &str = "hive.log4j2.xml";

/// The Vector agent configuration (`vector.yaml`).
const VECTOR_CONFIG: &str = include_str!("vector.yaml");

/// Returns the Vector agent config (`vector.yaml`) content.
pub fn vector_config_file_content() -> String {
    VECTOR_CONFIG.to_owned()
}

/// Renders `log4j2.properties` for the Hive metastore container.
///
/// Returns `None` when the Hive container does not use the operator's automatic logging
/// configuration (i.e. a custom log ConfigMap is referenced instead), in which case no
/// `log4j2.properties` should be added to the rolegroup `ConfigMap`.
pub fn build_log4j2(hive_container: &ValidatedContainerLogConfigChoice) -> Option<String> {
    match hive_container {
        ValidatedContainerLogConfigChoice::Automatic(log_config) => Some(log4j2_config(log_config)),
        ValidatedContainerLogConfigChoice::Custom(_) => None,
    }
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

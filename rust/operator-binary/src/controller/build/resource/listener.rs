use snafu::{OptionExt, Snafu};
use stackable_operator::{
    crd::listener::v1alpha1::{Listener, ListenerPort, ListenerSpec},
    v2::types::kubernetes::ListenerClassName,
};

use crate::{
    controller::{ValidatedCluster, build::PLACEHOLDER_LISTENER_ROLE_GROUP},
    crd::{HIVE_PORT, HIVE_PORT_NAME, HiveRole},
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("{role} listener has no address"))]
    RoleListenerHasNoAddress { role: String },
    #[snafu(display("could not find port [{port_name}] for rolegroup listener {role}"))]
    NoServicePort { port_name: String, role: String },
}

// Builds the connection string with respect to the listener provided objects
pub fn build_listener_connection_string(
    listener_ref: Listener,
    role: &str,
) -> Result<String, Error> {
    // We only need the first address corresponding to the role
    let listener_address = listener_ref
        .status
        .and_then(|s| s.ingress_addresses?.into_iter().next())
        .context(RoleListenerHasNoAddressSnafu { role })?;
    let conn_str = format!(
        "thrift://{address}:{port}",
        address = listener_address.address,
        port = listener_address
            .ports
            .get(HIVE_PORT_NAME)
            .copied()
            .context(NoServicePortSnafu {
                port_name: HIVE_PORT_NAME,
                role
            })?
    );
    Ok(conn_str)
}

// Designed to build a listener per role
// In case of Hive we expect only one role: Metastore
pub fn build_role_listener(
    cluster: &ValidatedCluster,
    hive_role: &HiveRole,
    listener_class: &ListenerClassName,
) -> Listener {
    // The role listener is a role-level (not role-group-level) object, so there is no real
    // role-group name; "none" is used as a placeholder for the recommended labels.
    let metadata = cluster
        .object_meta(
            cluster.role_listener_name(hive_role),
            &PLACEHOLDER_LISTENER_ROLE_GROUP,
        )
        .build();

    let spec = ListenerSpec {
        class_name: Some(listener_class.to_string()),
        ports: Some(listener_ports()),
        ..Default::default()
    };

    Listener {
        metadata,
        spec,
        status: None,
    }
}

pub fn listener_ports() -> Vec<ListenerPort> {
    vec![ListenerPort {
        name: HIVE_PORT_NAME.to_owned(),
        port: HIVE_PORT.into(),
        protocol: Some("TCP".to_owned()),
    }]
}

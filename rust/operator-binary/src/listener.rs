use snafu::{OptionExt, Snafu};
use stackable_operator::crd::listener::v1alpha1::Listener;

use crate::crd::HIVE_PORT_NAME;

// Listener volumes
pub const LISTENER_VOLUME_NAME: &str = "listener";
pub const LISTENER_VOLUME_DIR: &str = "/stackable/listener";

// Listener defaults
pub const DEFAULT_LISTENER_CLASS: &str = "cluster-internal";

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("{role} listener has no adress"))]
    RoleListenerHasNoAddress { role: String },
    #[snafu(display("could not find port [{port_name}] for rolegroup listener {role}"))]
    NoServicePort { port_name: String, role: String },
    #[snafu(display("chroot path {chroot} was relative (must be absolute)"))]
    RelativeChroot { chroot: String },
}

// Builds the connection string with respect to the listener provided objects
pub fn build_listener_connection_string(
    listener_ref: Listener,
    role: &String,
    chroot: Option<&str>,
) -> Result<String, Error> {
    // We only need the first address corresponding to the role
    let listener_address = listener_ref
        .status
        .and_then(|s| s.ingress_addresses?.into_iter().next())
        .context(RoleListenerHasNoAddressSnafu { role })?;
    let mut conn_str = format!(
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
    if let Some(chroot) = chroot {
        if !chroot.starts_with('/') {
            return RelativeChrootSnafu { chroot }.fail();
        }
        conn_str.push_str(chroot);
    }
    Ok(conn_str)
}

// used by crds to define a default listener_class name
pub fn metastore_default_listener_class() -> String {
    DEFAULT_LISTENER_CLASS.to_owned()
}

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    commons::product_image_selection::ResolvedProductImage,
    crd::listener::v1alpha1::{Listener, ListenerPort, ListenerSpec},
};

use crate::{
    controller::build_recommended_labels,
    crd::{HIVE_PORT, HIVE_PORT_NAME, HiveRole, v1alpha1},
};

// Listener volumes
pub const LISTENER_VOLUME_NAME: &str = "listener";
pub const LISTENER_VOLUME_DIR: &str = "/stackable/listener";

// Listener defaults
pub const DEFAULT_LISTENER_CLASS: &str = "cluster-internal";

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
    },
    #[snafu(display("failed to build Metadata"))]
    MetadataBuild {
        source: stackable_operator::builder::meta::Error,
    },
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

// Designed to build a listener per role
// In case of Hive we expect only one role: Metastore
pub fn build_group_listener(
    hive: &v1alpha1::HiveCluster,
    resolved_product_image: &ResolvedProductImage,
    hive_role: &HiveRole,
    listener_class: &String,
) -> Result<Listener, Error> {
    let metadata = ObjectMetaBuilder::new()
        .name_and_namespace(hive)
        .name(hive.group_listener_name(hive_role))
        .ownerreference_from_resource(hive, None, Some(true))
        .context(ObjectMissingMetadataForOwnerRefSnafu)?
        .with_recommended_labels(build_recommended_labels(
            hive,
            &resolved_product_image.app_version_label,
            &hive_role.to_string(),
            "none",
        ))
        .context(MetadataBuildSnafu)?
        .build();

    let spec = ListenerSpec {
        class_name: Some(listener_class.to_owned()),
        ports: Some(listener_ports()),
        ..Default::default()
    };

    let listener = Listener {
        metadata,
        spec,
        status: None,
    };

    Ok(listener)
}

fn listener_ports() -> Vec<ListenerPort> {
    vec![ListenerPort {
        name: HIVE_PORT_NAME.to_owned(),
        port: HIVE_PORT.into(),
        protocol: Some("TCP".to_owned()),
    }]
}

// used by crds to define a default listener_class name
pub fn metastore_default_listener_class() -> String {
    DEFAULT_LISTENER_CLASS.to_owned()
}

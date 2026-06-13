//! Builders that turn a `ValidatedCluster` into Kubernetes resources.

use std::str::FromStr;

use stackable_operator::v2::types::operator::{ProductVersion, RoleGroupName};

// Placeholder role-group name used for the recommended labels of the role-level discovery
// `ConfigMap` (which is not tied to a single role group).
stackable_operator::constant!(pub(crate) PLACEHOLDER_DISCOVERY_ROLE_GROUP: RoleGroupName = "discovery");

// Placeholder role-group name used for the recommended labels of the role-level `Listener`
// (which is not tied to a single role group).
stackable_operator::constant!(pub(crate) PLACEHOLDER_LISTENER_ROLE_GROUP: RoleGroupName = "none");

// Placeholder product version used for labels on PVC templates, which cannot be modified once
// deployed. A constant value keeps the labels stable across version upgrades.
stackable_operator::constant!(pub(crate) UNVERSIONED_PRODUCT_VERSION: ProductVersion = "none");

pub mod command;
pub mod graceful_shutdown;
pub mod jvm;
pub mod kerberos;
pub mod opa;
pub mod properties;
pub mod resource;

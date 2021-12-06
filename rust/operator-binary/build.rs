use stackable_hive_crd::{DatabaseConnection, HiveCluster};
use stackable_operator::crd::CustomResourceExt;

fn main() -> Result<(), stackable_operator::error::Error> {
    built::write_built_file().expect("Failed to acquire build-time information");

    HiveCluster::write_yaml_schema("../../deploy/crd/hivecluster.crd.yaml")?;
    DatabaseConnection::write_yaml_schema("../../deploy/crd/databaseconnection.crd.yaml")?;

    Ok(())
}

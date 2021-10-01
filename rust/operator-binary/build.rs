use stackable_operator::crd::CustomResourceExt;
use stackable_hive_crd::commands::{Restart, Start, Stop};
use stackable_hive_crd::{HiveCluster, DatabaseConnection};

fn main() -> Result<(), stackable_operator::error::Error> {
    built::write_built_file().expect("Failed to acquire build-time information");

    HiveCluster::write_yaml_schema("../../deploy/crd/hivecluster.crd.yaml")?;
    DatabaseConnection::write_yaml_schema("../../deploy/crd/databaseconnection.crd.yaml")?;
    Restart::write_yaml_schema("../../deploy/crd/restart.crd.yaml")?;
    Start::write_yaml_schema("../../deploy/crd/start.crd.yaml")?;
    Stop::write_yaml_schema("../../deploy/crd/stop.crd.yaml")?;

    Ok(())
}

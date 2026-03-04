use serde::{Deserialize, Serialize};
use stackable_operator::{
    databases::{
        databases::{
            derby::DerbyConnection, mysql::MysqlConnection, postgresql::PostgresqlConnection,
        },
        drivers::jdbc::JDBCDatabaseConnection,
    },
    schemars::{self, JsonSchema},
};

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum MetadataDatabaseConnection {
    /// Use a PostgreSQL database
    Postgresql(PostgresqlConnection),

    /// Use a MySQL database.
    ///
    /// Please note that - due to license issues - we don't ship the mysql driver, you need to add
    /// it it yourself.
    /// See <https://docs.stackable.tech/home/stable/hive/usage-guide/database-driver/> for details.
    Mysql(MysqlConnection),

    /// Use an Apache Derby database
    Derby(DerbyConnection),
    // We don't support generic (yet?), as we need to tell the metastore the `--dbtype` on startup,
    // which is not known for generic connection.
    // Generic(GenericJDBCDatabaseConnection),
}

impl MetadataDatabaseConnection {
    pub fn as_jdbc_database_connection(&self) -> &dyn JDBCDatabaseConnection {
        match self {
            Self::Postgresql(p) => p,
            Self::Mysql(m) => m,
            Self::Derby(d) => d,
        }
    }

    /// Name of the database as it should be passed using the `--db-type` CLI argument to Hive
    pub fn as_hive_db_type(&self) -> &str {
        match self {
            MetadataDatabaseConnection::Postgresql(_) => "postgres",
            MetadataDatabaseConnection::Mysql(_) => "mysql",
            MetadataDatabaseConnection::Derby(_) => "derby",
        }
    }
}

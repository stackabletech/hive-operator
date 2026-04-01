use std::ops::Deref;

use serde::{Deserialize, Serialize};
use stackable_operator::{
    database_connections::{
        databases::{
            derby::DerbyConnection, mysql::MysqlConnection, postgresql::PostgresqlConnection,
        },
        drivers::jdbc::JdbcDatabaseConnection,
    },
    schemars::{self, JsonSchema},
};

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum MetadataDatabaseConnection {
    // Docs are on the struct
    Postgresql(PostgresqlConnection),

    /// Connection settings for a [MySQL](https://www.mysql.com/) database.
    ///
    /// Please note that - due to license issues - we don't ship the mysql driver, you need to add
    /// it it yourself.
    /// See <https://docs.stackable.tech/home/stable/hive/usage-guide/database-driver/> for details.
    Mysql(MysqlConnection),

    // Docs are on the struct
    Derby(DerbyConnection),
    // We don't support generic (yet?), as we need to tell the metastore the `--dbtype` on startup,
    // which is not known for generic connection.
    // Generic(GenericJdbcDatabaseConnection),
}

impl Deref for MetadataDatabaseConnection {
    type Target = dyn JdbcDatabaseConnection;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Postgresql(p) => p,
            Self::Mysql(m) => m,
            Self::Derby(d) => d,
        }
    }
}

impl MetadataDatabaseConnection {
    /// Name of the database as it should be passed using the `--db-type` CLI argument to Hive
    pub fn as_hive_db_type(&self) -> &str {
        match self {
            MetadataDatabaseConnection::Postgresql(_) => "postgres",
            MetadataDatabaseConnection::Mysql(_) => "mysql",
            MetadataDatabaseConnection::Derby(_) => "derby",
        }
    }
}

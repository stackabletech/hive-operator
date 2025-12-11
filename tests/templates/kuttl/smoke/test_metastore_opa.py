#!/usr/bin/env python3
from hive_metastore_client import HiveMetastoreClient
from hive_metastore_client.builders import (
    DatabaseBuilder,
    ColumnBuilder,
    SerDeInfoBuilder,
    StorageDescriptorBuilder,
    TableBuilder,
)
import argparse


def table(db_name, table_name, location):
    columns = [ColumnBuilder("id", "string", "col comment").build()]

    serde_info = SerDeInfoBuilder(
        serialization_lib="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    ).build()

    storage_descriptor = StorageDescriptorBuilder(
        columns=columns,
        location=location,
        input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        serde_info=serde_info,
        compressed=True,
    ).build()

    test_table = TableBuilder(
        db_name=db_name,
        table_name=table_name,
        storage_descriptor=storage_descriptor,
    ).build()

    return test_table


if __name__ == "__main__":
    all_args = argparse.ArgumentParser(
        description="Test hive-metastore-opa-authorizer and rego rules."
    )
    all_args.add_argument("-p", "--port", help="Metastore server port", default="9083")
    all_args.add_argument(
        "-d", "--database", help="Test DB name", default="db_not_allowed"
    )
    all_args.add_argument(
        "-m", "--metastore", help="The host or service to connect to", required=True
    )
    args = vars(all_args.parse_args())

    database_name = args["database"]
    port = args["port"]
    host = args["metastore"]

    # Creating database object using builder
    database = DatabaseBuilder(database_name).build()

    print(
        f"[INFO] Trying to access '{database_name}' which is expected to fail due to 'database_allow' authorization policy...!"
    )

    with HiveMetastoreClient(host, port) as hive_client:
        try:
            hive_client.create_database_if_not_exists(database)
        except Exception as e:
            print(f"[DENIED] {e}")
            print(
                f"[SUCCESS] Test hive-metastore-opa-authorizer succeeded. Could not access database '{database_name}'!"
            )
            exit(0)

        print(
            f"[ERROR] Test hive-metastore-opa-authorizer failed. Could access database '{database_name}'!"
        )
        exit(-1)

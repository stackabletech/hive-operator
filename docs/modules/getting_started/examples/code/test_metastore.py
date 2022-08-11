#!/usr/bin/env python3
from hive_metastore_client import HiveMetastoreClient
from hive_metastore_client.builders import (
    DatabaseBuilder,
    ColumnBuilder,
    SerDeInfoBuilder,
    StorageDescriptorBuilder,
    TableBuilder,
)
from thrift_files.libraries.thrift_hive_metastore_client.ttypes import (
    FieldSchema,
    AlreadyExistsException,
)
import argparse


def table(db_name, table_name, location):
    columns = [
        ColumnBuilder("id", "string", "col comment").build()
    ]

    serde_info = SerDeInfoBuilder(
        serialization_lib="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    ).build()

    storage_descriptor = StorageDescriptorBuilder(
        columns=columns,
        location=location,
        input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        serde_info=serde_info,
    ).build()

    test_table = TableBuilder(
        db_name=db_name,
        table_name=table_name,
        storage_descriptor=storage_descriptor,
    ).build()

    return test_table


if __name__ == '__main__':
    all_args = argparse.ArgumentParser(description="Test hive metastore.")
    all_args.add_argument("-p", "--port", help="Metastore server port", default="9083")
    all_args.add_argument("-d", "--database", help="Test DB name", default="test_metastore")
    all_args.add_argument("-n", "--namespace", help="The namespace to run in", required=True)
    args = vars(all_args.parse_args())

    namespace = args["namespace"]
    database_name = args["database"]
    port = args["port"]
    local_test_table_name = "one_column_table"
    s3_test_table_name = "s3_one_column_table"
    s3_test_table_name_wrong_bucket = "s3_one_column_table_wrong_buckets"
    host = 'hive-postgres-s3-metastore-default-0.hive-postgres-s3-metastore-default.' + namespace + '.svc.cluster.local'
    # Creating database object using builder
    database = DatabaseBuilder(database_name).build()

    with HiveMetastoreClient(host, port) as hive_client:
        hive_client.create_database_if_not_exists(database)

        # Local access
        try:
            hive_client.create_table(table(database_name, local_test_table_name, f"/stackable/warehouse/location_{database_name}_{local_test_table_name}"))
        except AlreadyExistsException:
            print(f"[INFO]: Table {local_test_table_name} already existed")
        schema = hive_client.get_schema(db_name=database_name, table_name=local_test_table_name)
        expected = [FieldSchema(name='id', type='string', comment='col comment')]
        if schema != expected:
            print("[ERROR]: Received local schema " + str(schema) + " - expected schema: " + expected)
            exit(-1)

        # S3 access
        try:
            hive_client.create_table(table(database_name, s3_test_table_name, "s3a://hive/"))
        except AlreadyExistsException:
            print(f"[INFO]: Table {s3_test_table_name} already existed")
        schema = hive_client.get_schema(db_name=database_name, table_name=s3_test_table_name)
        expected = [FieldSchema(name='id', type='string', comment='col comment')]
        if schema != expected:
            print("[ERROR]: Received s3 schema " + str(schema) + " - expected schema: " + expected)
            exit(-1)

        print("[SUCCESS] Test finished successfully!")
        exit(0)

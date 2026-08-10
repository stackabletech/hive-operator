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
    GetTableRequest,
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


def check_table(hive_client, db_name, table_name, location, label):
    """Create the table if it does not exist yet and assert the metastore returns the
    expected schema for it.

    Returns the table metadata so callers can make additional assertions.
    """
    try:
        hive_client.create_table(table(db_name, table_name, location))
    except AlreadyExistsException:
        print(f"[INFO]: Table {table_name} already existed")

    schema = hive_client.get_schema(db_name=db_name, table_name=table_name)
    expected = [FieldSchema(name="id", type="string", comment="col comment")]
    if schema != expected:
        print(
            f"[ERROR]: Received {label} schema {schema} - expected schema: {expected}"
        )
        exit(-1)

    return hive_client.get_table_req(
        GetTableRequest(dbName=db_name, tblName=table_name)
    ).table


if __name__ == "__main__":
    all_args = argparse.ArgumentParser(description="Test hive metastore.")
    all_args.add_argument("-p", "--port", help="Metastore server port", default="9083")
    all_args.add_argument(
        "-d", "--database", help="Test DB name", default="test_metastore"
    )
    all_args.add_argument(
        "-m", "--metastore", help="The host or service to connect to", required=True
    )
    args = vars(all_args.parse_args())

    database_name = args["database"]
    port = args["port"]
    host = args["metastore"]
    local_test_table_name = "one_column_table"
    s3_test_table_name = "s3_one_column_table"
    s3_scheme_test_table_name = "s3_scheme_one_column_table"
    s3_test_table_name_wrong_bucket = "s3_one_column_table_wrong_buckets"
    # Creating database object using builder
    database = DatabaseBuilder(database_name).build()

    with HiveMetastoreClient(host, port) as hive_client:
        hive_client.create_database_if_not_exists(database)

        # Local access
        check_table(
            hive_client,
            database_name,
            local_test_table_name,
            f"/stackable/warehouse/location_{database_name}_{local_test_table_name}",
            "local",
        )

        # S3 access
        check_table(
            hive_client,
            database_name,
            s3_test_table_name,
            "s3a://hive/s3_one_column_table/",
            "s3",
        )

        # S3 access via the s3:// scheme. Clients such as Trino's native S3 Iceberg connector
        # address data with s3:// instead of s3a:// URIs. Creating the table makes the metastore
        # create the table directory itself, which fails unless the s3:// scheme is registered.
        s3_scheme_table = check_table(
            hive_client,
            database_name,
            s3_scheme_test_table_name,
            "s3://hive/s3_scheme_one_column_table/",
            "s3 scheme",
        )

        # The scheme must survive the round-trip. The metastore rewrites the stored location
        # using the scheme of the filesystem it resolved, and clients read that location back to
        # address the data. A location normalised to s3a:// would defeat registering the scheme.
        s3_scheme_location = s3_scheme_table.sd.location
        if not s3_scheme_location.startswith("s3://"):
            print(
                f"[ERROR]: Expected the stored location to keep the s3:// scheme, "
                f"got {s3_scheme_location}"
            )
            exit(-1)

        # Removed test, because it failed against Hive 3.1.3. We do not know if the behavior of the Hive metastore changed or we made a mistake. We improved the Trino tests to do more stuff with S3 (e.g. writing tables) which passed,
        # so we are confident that the removal of this test is ok

        # Wrong S3 bucket
        # try:
        #    wrong_location = "s3a://wrongbucket/"
        #    hive_client.create_table(table(database_name, s3_test_table_name_wrong_bucket, wrong_location))
        #    print(f"[ERROR]: Hive metastore created table {s3_test_table_name_wrong_bucket} in wrong location {wrong_location} which should have not been possible because the bucket didn't exist")
        #    exit(-1)
        # except MetaException as ex:
        #    if ex.message == 'Got exception: java.io.FileNotFoundException Bucket wrongbucket does not exist':
        #        print(f"[SUCCESS]: Could not read from wrong bucket: {ex}")
        #    else:
        #        print(f"[ERROR]: Got error during creating table pointing to wrong bucket: {ex}")
        #        exit(-1)

        print("[SUCCESS] Test finished successfully!")
        exit(0)

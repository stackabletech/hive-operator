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
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport
import argparse


class KerberosHiveMetastoreClient(HiveMetastoreClient):
    @staticmethod
    def _init_protocol(host: str, port: int) -> TBinaryProtocol:
        transport = TSocket.TSocket(host, int(port))
        # host is something like "hive-metastore-default-0.hive-metastore-default.kuttl-test-brave-caribou.svc.cluster.local"
        # We need to change it to the host part of the HMS principal e.g. "hive.kuttl-test-brave-caribou.svc.cluster.local"
        host = host.replace("hive-metastore-default-0.hive-metastore-default", "hive")
        transport = TTransport.TSaslClientTransport(transport,
                                                    # host part of the HMS principal (not the HMS client)
                                                    host=host,
                                                    # first part of the HMS principal (not the HMS client)
                                                    service="hive")
        transport = TTransport.TBufferedTransport(transport)
        return TBinaryProtocol.TBinaryProtocol(transport)


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
    all_args.add_argument("-m", "--metastore", help="The host or service to connect to", required=True)
    args = vars(all_args.parse_args())

    database_name = args["database"]
    port = args["port"]
    host = args["metastore"]
    local_test_table_name = "one_column_table"
    hdfs_test_table_name = "hdfs_one_column_table"
    # Creating database object using builder
    database = DatabaseBuilder(database_name).build()

    with KerberosHiveMetastoreClient(host, port) as hive_client:
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

        # HDFS access
        try:
            hive_client.create_table(table(database_name, hdfs_test_table_name, "hdfs://hdfs/access-hive/hive"))
        except AlreadyExistsException:
            print(f"[INFO]: Table {hdfs_test_table_name} already existed")
        schema = hive_client.get_schema(db_name=database_name, table_name=hdfs_test_table_name)
        expected = [FieldSchema(name='id', type='string', comment='col comment')]
        if schema != expected:
            print("[ERROR]: Received HDFS schema " + str(schema) + " - expected schema: " + expected)
            exit(-1)

        print("[SUCCESS] Test finished successfully!")
        exit(0)

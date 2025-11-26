```bash
k apply -f secrets.yaml
helm install postgresql oci://registry-1.docker.io/bitnamicharts/postgresql \
  --version 16.5.0 \
  --namespace default \
  --set image.repository=bitnamilegacy/postgresql \
  --set volumePermissions.image.repository=bitnamilegacy/os-shell \
  --set metrics.image.repository=bitnamilegacy/postgres-exporter \
  --set global.security.allowInsecureImages=true \
  --set auth.username=hive \
  --set auth.password=hive \
  --set auth.database=hive \
  --set primary.extendedConfiguration="password_encryption=md5" \
  --wait

helm install minio --version 12.6.4 -f helm-bitnami-minio-values.yaml --repo https://charts.bitnami.com/bitnami minio
k apply -f hive-metastore.yaml
k apply -f trino.yaml
```

Create tables:
```sql
CREATE SCHEMA IF NOT EXISTS lakehouse.tpch;
USE lakehouse.tpch;
create table if not exists nation as select * from tpch.sf1.nation;
create table if not exists region as select * from tpch.sf1.region;
create table if not exists part as select * from tpch.sf1.part;
create table if not exists partsupp as select * from tpch.sf1.partsupp;
create table if not exists supplier as select * from tpch.sf1.supplier;
create table if not exists customer as select * from tpch.sf1.customer;
create table if not exists orders as select * from tpch.sf1.orders;
create table if not exists lineitem as select * from tpch.sf1.lineitem;

--- Follow the rest of https://docs.stackable.tech/home/stable/demos/trino-iceberg/ :)
```

Findings:

1. http://localhost:9001/iceberg/v1/config is missing endpoints
   -> Fixed in 4.2.0 with https://github.com/apache/hive/commit/0fd1cee83f341b29d54b24a076dc4dbda7a5f320

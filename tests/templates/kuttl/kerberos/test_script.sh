#!/usr/bin/env bash

set -ex
klist -k /stackable/kerberos/keytab
kinit -kt /stackable/kerberos/keytab access-hive/access-hive.$NAMESPACE.svc.cluster.local
klist

export KERBEROS_REALM=$(grep -oP 'default_realm = \K.*' /stackable/kerberos/krb5.conf)
cat /stackable/conf/hdfs_mount/core-site.xml | sed -e 's/${env.KERBEROS_REALM}/'"$KERBEROS_REALM/g" > /stackable/conf/hive/core-site.xml
cat /stackable/conf/hdfs_mount/hdfs-site.xml | sed -e 's/${env.KERBEROS_REALM}/'"$KERBEROS_REALM/g" > /stackable/conf/hive/hdfs-site.xml

#!/usr/bin/env bash
set -euo pipefail

# The getting started guide script
# It uses tagged regions which are included in the documentation
# https://docs.asciidoctor.org/asciidoc/latest/directives/include-tagged-regions/
#
# There are two variants to go through the guide - using stackablectl or helm
# The script takes either 'stackablectl' or 'helm' as an argument
#
# The script can be run as a test as well, to make sure that the tutorial works
# It includes some assertions throughout, and at the end especially.

if [ $# -eq 0 ]
then
  echo "Installation method argument ('helm' or 'stackablectl') required."
  exit 1
fi

case "$1" in
"helm")
echo "Adding 'stackable-dev' Helm Chart repository"
# tag::helm-add-repo[]
helm repo add stackable-dev https://repo.stackable.tech/repository/helm-dev/
# end::helm-add-repo[]
echo "Installing Operators with Helm"
# tag::helm-install-operators[]
helm install --wait commons-operator stackable-dev/commons-operator --version 0.3.0-nightly
helm install --wait secret-operator stackable-dev/secret-operator --version 0.6.0-nightly
helm install --wait hive-operator stackable-dev/hive-operator --version 0.7.0-nightly
# end::helm-install-operators[]
;;
"stackablectl")
echo "installing Operators with stackablectl"
# tag::stackablectl-install-operators[]
stackablectl operator install \
  commons=0.3.0-nightly \
  secret=0.6.0-nightly \
  hive=0.7.0-nightly
# end::stackablectl-install-operators[]
;;
*)
echo "Need to give 'helm' or 'stackablectl' as an argument for which installation method to use!"
exit 1
;;
esac

###########################################################################
# Currently we cannot install minio / postgres via stackablectl (except for defining stacks).
# We will install this via helm for now

echo "Install minio certificates from minio-certificates.yaml"
# tag::install-hive-test-helper[]
kubectl apply -f minio-certificates.yaml
# end::install-hive-test-helper[]

echo "Install minio for S3"
helm install minio \
--namespace default \
--version 4.0.2 \
--set mode=standalone \
--set replicas=1 \
--set persistence.enabled=false \
--set buckets[0].name=hive,buckets[0].policy=none \
--set users[0].accessKey=hive,users[0].secretKey=hivehive,users[0].policy=readwrite \
--set resources.requests.memory=1Gi \
--set service.type=NodePort,service.nodePort=null \
--set consoleService.type=NodePort,consoleService.nodePort=null \
--set tls.enabled=true,tls.certSecret=minio-tls-certificates,tls.publicCrt=tls.crt,tls.privateKey=tls.key \
--repo https://charts.min.io/ minio

echo "Install postgres for Hive"
helm install hive \
--version=10 \
--namespace default \
--set postgresqlUsername=hive \
--set postgresqlPassword=hive \
--set postgresqlDatabase=hive \
--repo https://charts.bitnami.com/bitnami postgresql

###########################################################################

echo "Install Hive test helper from hive-test-helper.yaml"
# tag::install-hive-test-helper[]
kubectl apply -f hive-test-helper.yaml
# end::install-hive-test-helper[]

sleep 5

echo "Awaiting Hive test helper rollout finish"
# tag::watch-hive-test-helper-rollout[]
kubectl rollout status --watch statefulset/hive-test-helper
# end::watch-hive-test-helper-rollout[]

echo "Install HiveCluster from hive-postgres-s3.yaml"
# tag::install-hive[]
kubectl apply -f hive-postgres-s3.yaml
# end::install-hive[]

sleep 5

echo "Awaiting Hive rollout finish"
# tag::watch-hive-rollout[]
kubectl rollout status --watch statefulset/hive-postgres-s3-metastore-default
# end::watch-hive-rollout[]

# tag::submit-job[]
kubectl cp -n default ./test_metastore.py  hive-test-helper-0:/tmp
kubectl cp -n default ./requirements.txt hive-test-helper-0:/tmp
kubectl exec -n default hive-test-helper-0 -- pip install --user -r /tmp/requirements.txt
kubectl exec -n default hive-test-helper-0 -- python /tmp/test_metastore.py -n default
# end::submit-job[]

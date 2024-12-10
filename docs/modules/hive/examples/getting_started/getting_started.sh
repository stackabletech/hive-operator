#!/usr/bin/env bash
set -euo pipefail

# DO NOT EDIT THE SCRIPT
# Instead, update the j2 template, and regenerate it for dev with `make render-docs`.

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

cd "$(dirname "$0")"

case "$1" in
"helm")
echo "Adding 'stackable-dev' Helm Chart repository"
# tag::helm-add-repo[]
helm repo add stackable-dev https://repo.stackable.tech/repository/helm-dev/
# end::helm-add-repo[]
echo "Updating Helm repo"
helm repo update

echo "Installing Operators with Helm"
# tag::helm-install-operators[]
helm install --wait commons-operator stackable-dev/commons-operator --version 0.0.0-dev
helm install --wait secret-operator stackable-dev/secret-operator --version 0.0.0-dev
helm install --wait listener-operator stackable-dev/listener-operator --version 0.0.0-dev
helm install --wait hive-operator stackable-dev/hive-operator --version 0.0.0-dev
# end::helm-install-operators[]

echo "Install minio for S3"
# tag::helm-install-minio[]
helm install minio \
--version 4.0.2 \
--namespace default \
--set mode=standalone \
--set replicas=1 \
--set persistence.enabled=false \
--set "buckets[0].name=hive,buckets[0].policy=none" \
--set "users[0].accessKey=hive,users[0].secretKey=hivehive,users[0].policy=readwrite" \
--set resources.requests.memory=1Gi \
--set service.type=NodePort,service.nodePort=null \
--set consoleService.type=NodePort,consoleService.nodePort=null \
--repo https://charts.min.io/ minio
# end::helm-install-minio[]

echo "Install postgres for Hive"
# tag::helm-install-postgres[]
helm install postgresql \
--version 12.1.5 \
--namespace default \
--set auth.username=hive \
--set auth.password=hive \
--set auth.database=hive \
--set primary.extendedConfiguration="password_encryption=md5" \
--repo https://charts.bitnami.com/bitnami postgresql
# end::helm-install-postgres[]
;;
"stackablectl")

# This step will be omitted since the operators are installed via the stack definition
# It is just kept for documentation purposes here
if false; then
echo "Installing Operators with stackablectl"
# tag::stackablectl-install-operators[]
stackablectl operator install \
  commons=0.0.0-dev \
  secret=0.0.0-dev \
  listener=0.0.0-dev \
  hive=0.0.0-dev
# end::stackablectl-install-operators[]
fi

echo "Installing MinIO and PostgreSQL with stackablectl"
# tag::stackablectl-install-minio-postgres-stack[]
stackablectl \
--stack-file stackablectl-hive-postgres-minio-stack.yaml \
--release-file release.yaml \
stack install hive-minio-postgres
# end::stackablectl-install-minio-postgres-stack[]
;;
*)
echo "Need to provide 'helm' or 'stackablectl' as an argument for which installation method to use!"
exit 1
;;
esac

echo "Install HiveCluster"
# tag::install-hive[]
kubectl apply -f hive-minio-credentials.yaml
kubectl apply -f hive-minio-credentials-secret-class.yaml
kubectl apply -f hive-minio-s3-connection.yaml
kubectl apply -f hive-postgres-s3.yaml
# end::install-hive[]

sleep 15

echo "Awaiting Hive rollout finish"
# tag::watch-hive-rollout[]
kubectl rollout status --watch --timeout=8m statefulset/hive-postgres-s3-metastore-default
# end::watch-hive-rollout[]

echo "Install Hive test helper from hive-test-helper.yaml"
kubectl apply -f hive-test-helper.yaml

sleep 15

echo "Awaiting Hive test helper rollout finish"
kubectl rollout status --watch --timeout=5m statefulset/hive-test-helper

# Only for testing the cluster (not required for documentation)
echo "Running test scripts"
kubectl cp -n default ../../../../../tests/templates/kuttl/smoke/test_metastore.py hive-test-helper-0:/tmp
kubectl exec -n default hive-test-helper-0 -- python /tmp/test_metastore.py -m hive-postgres-s3-metastore-default-0.hive-postgres-s3-metastore-default.default.svc.cluster.local

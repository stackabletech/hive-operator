# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added

- Adds new telemetry CLI arguments and environment variables ([#596]).
  - Use `--file-log-max-files` (or `FILE_LOG_MAX_FILES`) to limit the number of log files kept.
  - Use `--file-log-rotation-period` (or `FILE_LOG_ROTATION_PERIOD`) to configure the frequency of rotation.
  - Use `--console-log-format` (or `CONSOLE_LOG_FORMAT`) to set the format to `plain` (default) or `json`.
- BREAKING: Add Listener support for Hive ([#605]).

### Changed

- BREAKING: Replace stackable-operator `initialize_logging` with stackable-telemetry `Tracing` ([#585], [#592], [#596]).
  - The console log level was set by `HIVE_OPERATOR_LOG`, and is now set by `CONSOLE_LOG_LEVEL`.
  - The file log level was set by `HIVE_OPERATOR_LOG`, and is now set by `FILE_LOG_LEVEL`.
  - The file log directory was set by `HIVE_OPERATOR_LOG_DIRECTORY`, and is now set
    by `FILE_LOG_DIRECTORY` (or via `--file-log-directory <DIRECTORY>`).
  - Replace stackable-operator `print_startup_string` with `tracing::info!` with fields.
- BREAKING: Inject the vector aggregator address into the vector config using the env var `VECTOR_AGGREGATOR_ADDRESS` instead
    of having the operator write it to the vector config ([#589]).
- test: Bump to Vector `0.46.1` ([#599]).
- BREAKING: Previously this operator would hardcode the UID and GID of the Pods being created to 1000/0, this has changed now ([#603])
  - The `runAsUser` and `runAsGroup` fields will not be set anymore by the operator
  - The defaults from the docker images itself will now apply, which will be different from 1000/0 going forward
  - This is marked as breaking because tools and policies might exist, which require these fields to be set
- Use versioned common structs ([#604]).

### Fixed

- Use `json` file extension for log files ([#591]).
- Fix a bug where changes to ConfigMaps that are referenced in the HiveCluster spec didn't trigger a reconciliation ([#589]).

[#585]: https://github.com/stackabletech/hive-operator/pull/585
[#589]: https://github.com/stackabletech/hdfs-operator/pull/589
[#591]: https://github.com/stackabletech/hive-operator/pull/591
[#592]: https://github.com/stackabletech/hive-operator/pull/592
[#596]: https://github.com/stackabletech/hive-operator/pull/596
[#599]: https://github.com/stackabletech/hive-operator/pull/599
[#603]: https://github.com/stackabletech/hive-operator/pull/603
[#604]: https://github.com/stackabletech/hive-operator/pull/604
[#605]: https://github.com/stackabletech/hive-operator/pull/605

## [25.3.0] - 2025-03-21

### Added

- Run a `containerdebug` process in the background of each Hive container to collect debugging information ([#554]).
- Aggregate emitted Kubernetes events on the CustomResources ([#560]).
- Support configuring JVM arguments ([#572]).
- Support for S3 region ([#574]).
- Support for version `4.0.1` as LTS ([#579]).

### Changed

- Default to OCI for image metadata and product image selection ([#561]).
- Increase default memory reservation to `768Mi` to avoid `OOMKilled` ([#578]).
- Mark version `4.0.1` as experimental and set `4.0.0` as LTS ([#582]).

### Fixed

- BREAKING: Remove the `hive-env.sh` config file, as e.g. setting `HADOOP_OPTS` in there had absolutely no effect.
  This is considered a fix, as users expected the envs to be used, but they haven't.
  Users should use `envOverrides` instead, which are actually working ([#572]).
- BREAKING: The env variable `HADOOP_HEAPSIZE` was previously put in `hive-env.sh` and very likely had no effect.
  It is now passed as env variable, thus working.
  This might impact your stacklet as the heap size setting now actually has an effect ([#572]).

[#554]: https://github.com/stackabletech/hive-operator/pull/554
[#560]: https://github.com/stackabletech/hive-operator/pull/560
[#561]: https://github.com/stackabletech/hive-operator/pull/561
[#572]: https://github.com/stackabletech/hive-operator/pull/572
[#574]: https://github.com/stackabletech/hive-operator/pull/574
[#578]: https://github.com/stackabletech/hive-operator/pull/578
[#579]: https://github.com/stackabletech/hive-operator/pull/579
[#582]: https://github.com/stackabletech/hive-operator/pull/582

## [24.11.1] - 2025-01-10

### Fixed

- BREAKING: Use distinct ServiceAccounts for the Stacklets, so that multiple Stacklets can be
  deployed in one namespace. Existing Stacklets will use the newly created ServiceAccounts after
  restart ([#544]).

[#544]: https://github.com/stackabletech/hive-operator/pull/544

## [24.11.0] - 2024-11-18

### Added

- Add support for Hive `4.0.0` ([#508]).
- The operator can now run on Kubernetes clusters using a non-default cluster domain.
  Use the env var `KUBERNETES_CLUSTER_DOMAIN` or the operator Helm chart property `kubernetesClusterDomain` to set a non-default cluster domain ([#522]).

### Changed

- Reduce CRD size from `487KB` to `60KB` by accepting arbitrary YAML input instead of the underlying schema for the following fields ([#505]):
  - `podOverrides`
  - `affinity`
- Use [`config-utils`](https://github.com/stackabletech/config-utils/) ([#518]).

### Fixed

- BREAKING: The fields `connection` and `host` on `S3Connection` as well as `bucketName` on `S3Bucket`are now mandatory ([#518]).
- An invalid `HiveCluster` doesn't cause the operator to stop functioning ([#523]).
- Fix upgrade path from HMS `3.3.x` to `4.0.x`. Previously the schemaTool would try to re-create the database tables and would therefore fail. Starting with version `4.0.0` the schemaTool has the flag `-initOrUpgradeSchema`, which we use to resolve that problem ([#539]).

[#505]: https://github.com/stackabletech/hive-operator/pull/505
[#508]: https://github.com/stackabletech/hive-operator/pull/508
[#518]: https://github.com/stackabletech/hive-operator/pull/518
[#522]: https://github.com/stackabletech/hive-operator/pull/522
[#523]: https://github.com/stackabletech/hive-operator/pull/523
[#539]: https://github.com/stackabletech/hive-operator/pull/539

## [24.7.0] - 2024-07-24

### Added

- Added documentation/tutorial on using external database drivers ([#449]).

### Changed

- BREAKING: Switch to new image that only contains HMS.
  For most of the users this is an internal change, but this is breaking for users of custom logging configurations as
  the key `hive-log4j2.properties` in the ConfigMap containing the logging configuration must now be called
  `metastore-log4j2.properties` ([#447]).
- Bump `stackable-operator` from `0.64.0` to `0.70.0` ([#480]).
- Bump `product-config` from `0.6.0` to `0.7.0` ([#480]).
- Bump other dependencies ([#483]).

### Fixed

- [BREAKING] Move the metastore `user` and `password` DB credentials out of the CRD into a Secret containing the keys `username` and `password` ([#452]).
- Processing of corrupted log events fixed; If errors occur, the error
  messages are added to the log event ([#472]).

[#447]: https://github.com/stackabletech/hive-operator/pull/447
[#449]: https://github.com/stackabletech/hive-operator/pull/449
[#452]: https://github.com/stackabletech/hive-operator/pull/452
[#472]: https://github.com/stackabletech/hive-operator/pull/472
[#480]: https://github.com/stackabletech/hive-operator/pull/480
[#483]: https://github.com/stackabletech/hive-operator/pull/483

## [24.3.0] - 2024-03-20

### Added

- Various documentation of the CRD ([#394]).
- Support user authentication using Kerberos ([#402]).
- Helm: support labels in values.yaml ([#406]).

[#394]: https://github.com/stackabletech/hive-operator/pull/394
[#402]: https://github.com/stackabletech/hive-operator/pull/402
[#406]: https://github.com/stackabletech/hive-operator/pull/406

## [23.11.0] - 2023-11-24

### Added

- Default stackableVersion to operator version ([#360]).
- Configuration overrides for the JVM security properties, such as DNS caching ([#365]).
- Support PodDisruptionBudgets ([#376]).
- Support graceful shutdown ([#385]).

### Changed

- `vector` `0.26.0` -> `0.33.0` ([#361], [#377]).
- `operator-rs` `0.44.0` -> `0.55.0` ([#360], [#376], [#377]).
- jmx-exporter now referenced via soft link without version ([#377]).
- Service discovery now exposes the cluster service to enable HA ([#382]).

### Removed

- Support for 2.3.9 ([#377]).

[#360]: https://github.com/stackabletech/hive-operator/pull/360
[#361]: https://github.com/stackabletech/hive-operator/pull/361
[#365]: https://github.com/stackabletech/hive-operator/pull/365
[#376]: https://github.com/stackabletech/hive-operator/pull/376
[#377]: https://github.com/stackabletech/hive-operator/pull/377
[#382]: https://github.com/stackabletech/hive-operator/pull/382
[#385]: https://github.com/stackabletech/hive-operator/pull/385

## [23.7.0] - 2023-07-14

### Added

- Generate OLM bundle for Release 23.4.0 ([#338]).
- Missing CRD defaults for `status.conditions` field ([#340]).
- Set explicit resources on all container ([#345], [#347])
- Support podOverrides ([#352])

### Fixed

- Increase the size limit of the log volume ([#354]).

### Changed

- Operator-rs: `0.40.2` -> `0.44.0` ([#336], [#354]).
- Use 0.0.0-dev product images for testing ([#337])
- Use testing-tools 0.2.0 ([#337])
- Added kuttl test suites ([#348])

[#336]: https://github.com/stackabletech/hive-operator/pull/336
[#337]: https://github.com/stackabletech/hive-operator/pull/337
[#338]: https://github.com/stackabletech/hive-operator/pull/338
[#340]: https://github.com/stackabletech/hive-operator/pull/340
[#345]: https://github.com/stackabletech/hive-operator/pull/345
[#347]: https://github.com/stackabletech/hive-operator/pull/347
[#348]: https://github.com/stackabletech/hive-operator/pull/348
[#352]: https://github.com/stackabletech/hive-operator/pull/352
[#354]: https://github.com/stackabletech/hive-operator/pull/354

## [23.4.0] - 2023-04-17

### Added

- Deploy default and support custom affinities ([#315]).
- Openshift compatibility ([#323]).
- Incorporated cluster-operation change. ([#323]).
- Extend cluster resources for status and cluster operation (paused, stopped) ([#324]).
- Cluster status conditions ([#326]).

### Changed

- [BREAKING]: Support specifying Service type by moving `serviceType` (which was an experimental feature) to `clusterConfig.listenerClass`.
  This enables us to later switch non-breaking to using `ListenerClasses` for the exposure of Services.
  This change is breaking, because - for security reasons - we default to the `cluster-internal` `ListenerClass`.
  If you need your cluster to be accessible from outside of Kubernetes you need to set `clusterConfig.listenerClass`
  to `external-unstable` or `external-stable` ([#327]).
- Use operator-rs `build_rbac_resources` method ([#323]).
- `operator-rs` `0.36.0` → `0.40.2` ([#323], [#324]).

### Fixes

- Bugfix: heap formatting and update product images used for tests ([#317])

[#315]: https://github.com/stackabletech/hive-operator/pull/315
[#317]: https://github.com/stackabletech/hive-operator/pull/317
[#323]: https://github.com/stackabletech/hive-operator/pull/323
[#324]: https://github.com/stackabletech/hive-operator/pull/324
[#326]: https://github.com/stackabletech/hive-operator/pull/326
[#327]: https://github.com/stackabletech/hive-operator/pull/327

## [23.1.0] - 2023-01-23

### Changed

- Updated stackable image versions ([#271]).
- `operator-rs` `0.25.2` → `0.32.1` ([#274], [#283], [#292], [#298]).
- Consolidated security context user, group and fs group ([#277]).
- [BREAKING] Use Product image selection instead of version. `spec.version` has been replaced by `spec.image` ([#280]).
- Fix role group node selector ([#283]).
- [BREAKING] Moved `database` specification from role / role-group level to top-level `clusterConfig` ([#292]).
- [BREAKING] Moved `s3`, `serviceType` and `hdfs` discovery to top-level `clusterConfig` ([#292]).
- Enable logging ([#298]).

[#271]: https://github.com/stackabletech/hive-operator/pull/271
[#274]: https://github.com/stackabletech/hive-operator/pull/274
[#277]: https://github.com/stackabletech/hive-operator/pull/277
[#280]: https://github.com/stackabletech/hive-operator/pull/280
[#283]: https://github.com/stackabletech/hive-operator/pull/283
[#292]: https://github.com/stackabletech/hive-operator/pull/292
[#298]: https://github.com/stackabletech/hive-operator/pull/298

## [0.8.0] - 2022-11-07

### Added

- PVCs for data storage, cpu and memory limits are now configurable ([#242]).
- Orphaned resources are deleted ([#254]).
- Support HDFS connections ([#264]).

### Changed

- `operator-rs` `0.22.0` -> `0.25.2` ([#254]).

[#242]: https://github.com/stackabletech/hive-operator/pull/242
[#254]: https://github.com/stackabletech/hive-operator/pull/254
[#264]: https://github.com/stackabletech/hive-operator/pull/264

## [0.7.0] - 2022-09-06

### Added

- Improved, tested getting started guide via script ([#225]).
- Add temporary attribute to support using ClusterIP instead of NodePort service type ([#237]).

### Changed

- Include chart name when installing with a custom release name ([#204], [#205]).
- `operator-rs` `0.21.1` -> `0.22.0` ([#206]).
- Add support for Hive 3.1.3 ([#211], [#213]).

### Fixed

- Add missing role to read S3Connection objects ([#220]).

[#204]: https://github.com/stackabletech/hive-operator/pull/204
[#205]: https://github.com/stackabletech/hive-operator/pull/205
[#206]: https://github.com/stackabletech/hive-operator/pull/206
[#211]: https://github.com/stackabletech/hive-operator/pull/211
[#213]: https://github.com/stackabletech/hive-operator/pull/213
[#220]: https://github.com/stackabletech/hive-operator/pull/220
[#225]: https://github.com/stackabletech/hive-operator/pull/225
[#237]: https://github.com/stackabletech/hive-operator/pull/237

## [0.6.0] - 2022-06-30

### Added

- Reconciliation errors are now reported as Kubernetes events ([#137]).
- Use cli argument `watch-namespace` / env var `WATCH_NAMESPACE` to specify
  a single namespace to watch ([#142]).
- Warning in docs to use only PostgreSQL <= 10 ([#168]).
- Support S3 TLS verification ([#198]).

### Changed

- `operator-rs` `0.10.0` -> `0.21.0` ([#137], [#142], [#168], [#179]).
- Adapted S3 connection to operator-rs provided structs ([#179]).
- [BREAKING] Specifying the product version has been changed to adhere to [ADR018](https://docs.stackable.tech/home/contributor/adr/ADR018-product_image_versioning.html) instead of just specifying the product version you will now have to add the Stackable image version as well, so `version: 2.3.9` becomes (for example) `version: 2.3.9-stackable0.4.0` ([#184])

[#137]: https://github.com/stackabletech/hive-operator/pull/137
[#142]: https://github.com/stackabletech/hive-operator/pull/142
[#168]: https://github.com/stackabletech/hive-operator/pull/168
[#179]: https://github.com/stackabletech/hive-operator/pull/179
[#184]: https://github.com/stackabletech/hive-operator/pull/184
[#198]: https://github.com/stackabletech/hive-operator/pull/198

## [0.5.0] - 2022-02-14

### Added

- monitoring scraping label `prometheus.io/scrape: true` ([#115]).

### Changed

- `operator-rs` `0.8.0` → `0.10.0` ([#115]).

[#115]: https://github.com/stackabletech/hive-operator/pull/115

## [0.4.0] - 2022-01-27

### Added

- Discovery via ConfigMaps ([#52]).
- Services and Nodeports ([#52]).

### Changed

- `operator-rs` `0.5.0` → `0.8.0` ([#52], [#73], [#85]).
- Migrated to StatefulSet rather than direct Pod management ([#52]).
- Changed version from enum to String ([#52]).
- Shut down gracefully ([#72]).

### Removed

- Command handling and respective CRDs ([#52]).
- Hive port and metrics port not configurable anymore and removed from CRD ([#52]).

[#52]: https://github.com/stackabletech/hive-operator/pull/52
[#72]: https://github.com/stackabletech/hive-operator/pull/72
[#73]: https://github.com/stackabletech/hive-operator/pull/73
[#85]: https://github.com/stackabletech/hive-operator/pull/85

## [0.3.0] - 2021-12-06

## [0.2.0] - 2021-11-12

### Changed

- `operator-rs` `0.3.0` → `0.4.0` ([#21]).
- Adapted pod image and container command to docker image ([#21]).
- Adapted documentation to represent new workflow with docker images ([#21]).

[#21]: https://github.com/stackabletech/hive-operator/pull/21

## [0.1.0] - 2021-10-27

### Changed

- operator-rs : 0.3.0 ([#14])
- Use framework re-exports. ([#14])

[#14]: https://github.com/stackabletech/hive-operator/pull/14

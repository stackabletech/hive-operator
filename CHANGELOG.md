# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added

- Generate OLM bundle for Release 23.4.0 ([#338]).
- Missing CRD defaults for `status.conditions` field ([#340]).
- Set explicit resources on all container ([#345], [#347])

### Changed

- Operator-rs: `0.40.2` -> `0.41.0` ([#336]).
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

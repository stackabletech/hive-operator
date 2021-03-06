# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Changed

- Include chart name when installing with a custom release name ([#204], [#205]).
- `operator-rs` `0.21.1` -> `0.22.0` ([#206]).
- Add support for Hive 3.1.3 ([#211], [#213])

[#204]: https://github.com/stackabletech/trino-operator/pull/204
[#205]: https://github.com/stackabletech/trino-operator/pull/205
[#206]: https://github.com/stackabletech/trino-operator/pull/206
[#211]: https://github.com/stackabletech/trino-operator/pull/211
[#213]: https://github.com/stackabletech/trino-operator/pull/213

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

# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- Add TokenManager Interface ([#7452](https://github.com/opensearch-project/OpenSearch/pull/7452))
- Implement concurrent aggregations support without profile option ([#7514](https://github.com/opensearch-project/OpenSearch/pull/7514))

### Dependencies
- Bump `com.azure:azure-storage-common` from 12.21.0 to 12.21.1 (#7566, #7814)
- Bump `com.google.guava:guava` from 30.1.1-jre to 32.0.0-jre (#7565, #7811, #7807, #7808)
- Bump `net.minidev:json-smart` from 2.4.10 to 2.4.11 (#7660, #7812)
- Bump `org.gradle.test-retry` from 1.5.2 to 1.5.3 (#7810)
- Bump `com.diffplug.spotless` from 6.17.0 to 6.18.0 (#7896)
- Bump `jackson` from 2.15.1 to 2.15.2 ([#7897](https://github.com/opensearch-project/OpenSearch/pull/7897))
- Add `com.github.luben:zstd-jni` version 1.5.5-3 ([#2996](https://github.com/opensearch-project/OpenSearch/pull/2996))
- Bump `netty` from 4.1.91.Final to 4.1.93.Final ([#7901](https://github.com/opensearch-project/OpenSearch/pull/7901))
- Bump `com.amazonaws` 1.12.270 to `software.amazon.awssdk` 2.20.55 ([7372](https://github.com/opensearch-project/OpenSearch/pull/7372/))
- Add `org.reactivestreams` 1.0.4 ([7372](https://github.com/opensearch-project/OpenSearch/pull/7372/))

### Changed
- Replace jboss-annotations-api_1.2_spec with jakarta.annotation-api ([#7836](https://github.com/opensearch-project/OpenSearch/pull/7836))
- Reduce memory copy in zstd compression ([#7681](https://github.com/opensearch-project/OpenSearch/pull/7681))
- Add min, max, average and thread info to resource stats in tasks API ([#7673](https://github.com/opensearch-project/OpenSearch/pull/7673))
- Add ZSTD compression for snapshotting ([#2996](https://github.com/opensearch-project/OpenSearch/pull/2996))
- Change `com.amazonaws.sdk.ec2MetadataServiceEndpointOverride` to `aws.ec2MetadataServiceEndpoint` ([7372](https://github.com/opensearch-project/OpenSearch/pull/7372/))
- Change `com.amazonaws.sdk.stsEndpointOverride` to `aws.stsEndpointOverride` ([7372](https://github.com/opensearch-project/OpenSearch/pull/7372/))

### Deprecated

### Removed

### Fixed
-  Fixing error: adding a new/forgotten parameter to the configuration for checking the config on startup in plugins/repository-s3 #7924 

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.8...2.x

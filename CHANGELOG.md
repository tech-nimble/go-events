# Changelog

All notable changes to this package are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project follows
[Semantic Versioning](https://semver.org/).

## [1.0.0]

First release under Nimble Tech.

### Added
- `initializers/amqp_v2` bootstrap helpers for RabbitMQ, Kafka and the event bus.
- Tests, golangci-lint config, GitHub Actions CI and dependabot.

### Changed
- Naming aligned with Go conventions (`Event.EntityID`, `Options.URL`).
- `interface{}` replaced with `any`.

### Fixed
- RPC `Request`/`Response` implement their interfaces instead of panicking.
- Connection close errors are explicitly handled.

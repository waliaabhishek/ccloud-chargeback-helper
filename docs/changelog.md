## Unreleased

### Added

- Add bounded self-managed Kafka historical Prometheus acquisition with a default
  five-day response/chunk limit, configurable `1..30` day range, exact scope reuse,
  recovery preflight, and documented logical-family versus HTTP-attempt bounds.
- Add opt-in quota-backed principal attribution for self-managed Kafka network
  pools, including fail-closed evidence handling, self-managed-plugin-owned
  scope state and historical team snapshots, and operator configuration
  guidance.
- Add optional self-managed Kafka Prometheus metric and label aliases plus an
  explicit telemetry checker with deterministic JSONL diagnostics and
  warning-only historical gaps.
- Reject negative and non-finite self-managed Kafka cost rates and pool telemetry;
  invalid UTC days fail closed, remain retryable, and do not create downstream
  billing or attribution output.
- Add a credential-free root `./demo` path with Docker Compose support for a
  deterministic rolling six-month Clean Confluent profile, a real API-only
  backend, and a React UI.
- Expand the Clean demo with selectable Confluent Cloud and self-managed Kafka
  tenants from one fictional company: a dense Confluent topology with four
  environments, six Kafka clusters, 120 topics, 16 connectors, and
  Schema Registry, ksqlDB, and Flink services, plus a self-managed topology
  with two clusters, 24 topics, 10 identities, and four teams. Generated tags,
  allocations, topic attribution, healthy pipeline status, exports, and
  Confluent Cloud FOCUS Mapping Preview are available through the normal UI and
  API using fully synthetic data without external provider calls.
- Add an opt-in `./demo --showcase` profile; bare `./demo` remains Clean. The
  deterministic additive profile provides comparison, trend, budget, anomaly,
  unit-economics, and partition-efficiency source conditions, with exact
  persisted validation beyond 10,000 rows.

--8<-- "CHANGELOG.md"

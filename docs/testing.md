# Suggested validation (not performed)

Unit tests cover bounded batching, empty input rejection, duplicate IDs, Monday/EST semantics, duplicate Airflow trigger handling, destination manifest verification and partial-publication rejection.

Before deployment add DAG import/serialization tests against pinned provider versions, control API contract tests, multi-client authorization tests, forced timeout-after-copy cases, multipart checksum failures, external job timeouts, Snowflake zero-row loads and history-expiry replays. Load-test mapping limits and metadata DB pressure with synthetic manifests. Record measured p95 durations before tuning concurrency.

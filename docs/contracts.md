# Integration contracts

## Control API (not implemented)
- POST /jobs accepts kind and run_id; durable idempotency key is (kind,run_id). Return {id}.
- GET /jobs/{id} returns state RUNNING, SUCCEEDED, FAILED or CANCELLED. dbt SUCCEEDED includes passing tests.
- POST /plans validates event business date and snapshot readiness; loads a client-isolated source manifest. Invoke discover for each enabled S3 client. Return {arguments:[["copy-batch",run_id,"--client",id,"--manifest-uri",uri],...]}. Reject global counts above 1024 and unsupported enabled destinations. Each manifest must contain source bucket, approved source prefixes, allowed destination bucket/prefix and immutable expected object metadata. No client can select another client's partition.
- POST /publish verifies expected receipts, destination object counts and checksums, then writes a deterministic per-client _SUCCESS manifest. Never publish based only on pod exit code.
- POST /reports reads every expected client from snapshotted config, including WAITING and FAILED states, records deadline compliance and emits alert on failure or threatened review buffer.

## Worker implementation required
Bulk S3 workers must stream server-side multipart copy, retry individual failed copies, verify target metadata and persist per-batch receipts before exiting successfully. Never load large datasets into memory. A timeout after successful copy retries the same immutable key. Cross-account destination writes require explicit trust, KMS policy and expected bucket owner checks. Receipt keys include client/run/batch; writes must reject conflicting payloads.

Snowflake worker must iterate or map approved client configs with isolated connections; confirm object delivery completion first, use client-isolated stage, call snowflake.load, inspect COPY result and query destination row counts/content checks. Persist verified result, not just submission success. Add a durable deduplication ledger and MERGE for replay beyond COPY history. The supplied entry point fails closed rather than simulating successful cloud delivery.

Raw extraction is represented by the event marker and Databricks Snowflake read. Original upstream ingestion systems are unspecified, so no fictional extractor is supplied. Production snapshot selection must pin ingestion version; the notebook's full-table read is illustrative and not snapshot-isolated across reruns.

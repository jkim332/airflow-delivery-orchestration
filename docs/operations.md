# Operating assumptions and deployment gaps

1. Package src into a pinned worker image and install it in the Airflow environment. Implement the worker/control contracts before scheduling; placeholders intentionally fail closed.
2. Install Airflow with official constraints and the Kubernetes provider. Use workload identities/secrets, Kubernetes service accounts and namespace quotas; never put credentials in XCom or manifests.
3. Provision `airflow pools set object_delivery 8 "Shared bandwidth and S3 quota"` and `airflow pools set snowflake_delivery 4 "Warehouse budget"`.
4. Snapshot client config and immutable manifest keys per run. Set Airflow max_map_length consistently with the plan ceiling. The supplied config is documentation: control service must explicitly load/enforce it.
5. Benchmark partition size, p95 transfer duration, retries, job latency and available throughput. Choose ingestion cutoff by working backwards from the review deadline plus recovery buffer. No capacity or SLA guarantee is asserted.
6. Export structured delivery events to a durable status store and dashboard. Alert on missing markers, failed external jobs, no-progress batches, pending clients and predicted deadline misses. Logs alone are not durable delivery state.
7. Run unit tests after adapting dependencies (`pip install -e '.[test]'`, `pytest`). Tests supplied here were not executed. Airflow import checks and integration tests with real staging destinations remain required.
8. Use independent reporting/alerting for job failures before delivery starts. The sample report downstream of Snowflake is not a complete event-based monitoring system. Preserve final all-success gate so reports cannot conceal failures.
9. Immutable output publication needs lifecycle cleanup and a client contract to ignore data without _SUCCESS. Re-exporting a Databricks run requires explicit reconciliation rather than overwriting a published snapshot.

No Terraform, cloud deployment, actual service connection or benchmark results are included.

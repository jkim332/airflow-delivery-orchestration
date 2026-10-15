# Multi-source data delivery pipeline

Portfolio implementation, not a deployed or end-to-end validated service. External jobs and destination access require environment-specific integration. No application code or tests were executed to produce this repository.

## 1. Business problem
Deliver client data by Monday 09:00 EST, leaving an explicit earlier quality-review buffer. Completion means verified destination availability, not merely a completed transformation.

## 2. Original pipeline and constraints
A readiness marker triggers Airflow. dbt transforms Snowflake data for analysts; Databricks reads raw Snowflake data, partitions it and writes delivery objects to S3. Cloud object deliveries run before Snowflake deliveries. Variable runtimes, quotas and shared bandwidth threaten the deadline.

The current reference describes approximately 500 GB per run, 50+ S3, 10+ Azure, 10+ GCS and 50+ Snowflake destinations. Its result section inconsistently says 500 GB per client. This example assumes 500 GB total per run; capacity planning must resolve that discrepancy. The request's older 300 GB assumption is not used.

## 3. Proposed architecture
See docs/architecture.md. Explicit dependencies implement [Azure, GCS, S3] -> Snowflake. KubernetesPodOperator isolates delivery compute from Airflow workers; pools and task limits still bound shared services.

## 4. Repository
- dags/: job polling, bounded dynamic mapping, explicit phase dependencies.
- src/delivery/: planning, event triggering, immutable S3 delivery, Snowflake load, status and deadlines.
- dbt/: representative source, transformation and quality tests.
- databricks/: raw export example.
- config/: synthetic clients and capacity settings.
- docs/: design, deployment requirements, failure handling.
- scripts/: generation of 120 synthetic destination records.


Reference: https://jkim332.github.io/joohakim/data-delivery.html

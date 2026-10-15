# Architecture
```mermaid
flowchart TD
  A[Raw folder SUCCESS/checksum] --> B[Authenticated event receiver]
  B --> C[Airflow API deterministic run ID]
  C --> D[dbt job and quality tests]
  D --> E[Databricks raw Snowflake export]
  E --> F[Immutable partitioned S3 snapshot]
  F --> G[Bounded batch manifests]
  G --> H[Object TaskGroup: Azure / GCS / S3]
  H --> I[Verify each client and publish completion]
  I --> J[Snowflake TaskGroup]
  J --> K[Per-client verification and SLA report]
```

```mermaid
sequenceDiagram
  Airflow->>Control: Submit idempotent external job
  loop Until timeout
    Airflow->>Control: Poll persisted job state
  end
  Airflow->>Kubernetes: Map bounded batch arguments
  Kubernetes->>S3: Read manifest and transfer assigned objects
  Kubernetes->>Control: Persist verified batch receipt
  Airflow->>Control: Validate all expected receipts then publish
```

Actual DAG object phase demonstrates S3; Azure/GCS adapters deliberately fail closed until implemented. The control plane and worker entry point are documented integration contracts, not an implemented service. Partition discovery and verification helpers are real example logic. Mapping completion forms a conservative global barrier; per-client finalizers are a production improvement to reduce unrelated blocking.

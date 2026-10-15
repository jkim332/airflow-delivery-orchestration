"""Airflow 2.10 example. Control API/worker image are deployment contracts."""

import os
from datetime import timedelta

import pendulum
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.task_group import TaskGroup
from kubernetes.client import models as k8s

CONTROL_API = os.environ.get("CONTROL_API", "https://control.example.invalid")


def call_control(path, method="GET", **kwargs):
    response = requests.request(
        method,
        CONTROL_API + path,
        timeout=30,
        headers={"Authorization": f"Bearer {os.environ['CONTROL_TOKEN']}"},
        **kwargs,
    )
    response.raise_for_status()
    return response.json()


def job_finished(job, **context):
    result = call_control(f"/jobs/{context['ti'].xcom_pull(task_ids=job)}")
    if result["state"] in {"FAILED", "CANCELLED"}:
        raise RuntimeError("external transformation failed")
    return result["state"] == "SUCCEEDED"


with DAG(
    "client_delivery",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=16,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
) as dag:

    @task
    def start_job(kind, **context):
        run_id = context["dag_run"].conf["run_id"]
        return call_control("/jobs", "POST", json={"kind": kind, "run_id": run_id})["id"]

    @task
    def plan_batches(**context):
        # Server invokes manifests.discover, snapshots client config, validates
        # Monday and returns <=128 batch argument arrays per enabled client.
        # It must reject unsupported Azure/GCS records, not silently omit them.
        return call_control("/plans", "POST", json=context["dag_run"].conf)["arguments"]

    @task
    def publish(**context):
        # Must verify receipts for every expected client batch before publishing.
        return call_control("/publish", "POST", json=context["dag_run"].conf)

    @task(trigger_rule="all_done", retries=0)
    def report(**context):
        return call_control("/reports", "POST", json=context["dag_run"].conf)

    dbt = start_job.override(task_id="dbt")(kind="dbt")
    dbt_done = PythonSensor(
        task_id="wait_dbt",
        python_callable=job_finished,
        op_kwargs={"job": "dbt"},
        mode="reschedule",
        poke_interval=60,
        timeout=14_400,
    )

    spark = start_job.override(task_id="databricks")(kind="databricks")
    spark_done = PythonSensor(
        task_id="wait_databricks",
        python_callable=job_finished,
        op_kwargs={"job": "databricks"},
        mode="reschedule",
        poke_interval=60,
        timeout=21_600,
    )

    dbt >> dbt_done >> spark >> spark_done

    arguments = plan_batches()
    spark_done >> arguments

    with TaskGroup("object_deliveries"):
        copies = KubernetesPodOperator.partial(
            task_id="copy_batch",
            name="delivery-batch",
            image="example.invalid/delivery:PIN_DIGEST",
            cmds=["python", "-m", "delivery.worker"],
            pool="object_delivery",
            max_active_tis_per_dag=8,
            get_logs=True,
            is_delete_operator_pod=True,
            container_resources=k8s.V1ResourceRequirements(
                requests={"cpu": "1", "memory": "2Gi"},
                limits={"cpu": "2", "memory": "4Gi"},
            ),
            execution_timeout=timedelta(hours=4),
        ).expand(arguments=arguments)

    published = publish()
    copies >> published

    with TaskGroup("snowflake_deliveries"):
        snowflake = KubernetesPodOperator(
            task_id="load_clients",
            name="snowflake-delivery",
            image="example.invalid/delivery:PIN_DIGEST",
            cmds=["python", "-m", "delivery.worker"],
            arguments=["snowflake", "{{ dag_run.conf['run_id'] }}"],
            pool="snowflake_delivery",
            get_logs=True,
            is_delete_operator_pod=True,
        )

    published >> snowflake

    reporting = report()
    snowflake >> reporting
    success = EmptyOperator(task_id="success", trigger_rule="all_success")
    [snowflake, reporting] >> success

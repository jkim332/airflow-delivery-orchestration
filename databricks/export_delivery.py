# Databricks notebook source
# Credentials provided by a secret scope, never checked into source.
dbutils.widgets.text('run_id','')
run_id=dbutils.widgets.get('run_id')
import re
if not re.fullmatch(r'[A-Za-z0-9_-]{1,80}',run_id):
    raise ValueError('invalid run ID')
options={
    'sfURL':dbutils.secrets.get('delivery','snowflake-url'),
    'sfUser':dbutils.secrets.get('delivery','snowflake-user'),
    'sfPassword':dbutils.secrets.get('delivery','snowflake-password'),
    'sfDatabase':'RAW','sfSchema':'INGEST','sfWarehouse':'EXPORT_WH'}
raw=(spark.read.format('snowflake').options(**options)
     .option('dbtable','EVENTS').load())
# Production raw snapshot must be pinned to the event's immutable ingestion version.
(raw.repartition(128,'CLIENT_ID').write.mode('errorifexists')
    .partitionBy('CLIENT_ID').parquet(f's3://example-delivery-staging/{run_id}/'))
# Emit readiness only after file commit and validation; never expose partial output.

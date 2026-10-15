import re

def identifier(value):
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',value):
        raise ValueError('invalid SQL identifier')
    return '"'+value.upper()+'"'

def load(connection, database, schema, table, stage, run_id):
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,80}',run_id):
        raise ValueError('invalid run ID')
    target='.'.join(map(identifier,[database,schema,table]))
    stage=identifier(stage)
    with connection.cursor() as cursor:
        cursor.execute(f"COPY INTO {target} FROM @{stage}/{run_id}/ "
                       "FILE_FORMAT=(TYPE=PARQUET) MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE FORCE=FALSE")
        result=cursor.fetchall()
    # Snowflake load history makes normal retry safe within its retention window.
    # Do not set FORCE=TRUE. Beyond that window use a durable run ledger and
    # deterministic MERGE keys; COPY history alone is not exactly-once delivery.
    return result

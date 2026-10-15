import json
from .planning import plan_batches

def discover(s3, bucket, prefix, manifest_bucket, manifest_prefix):
    partitions = set()
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket,Prefix=prefix):
        for item in page.get('Contents',[]):
            key=item['Key']
            if key.endswith('.parquet'):
                partitions.add(key.rsplit('/',1)[0]+'/')
    batches=plan_batches(partitions)
    for index, batch in enumerate(batches):
        s3.put_object(Bucket=manifest_bucket,
            Key=f'{manifest_prefix}/batch-{index}.json',
            Body=json.dumps({'partitions':batch}).encode())
    # Only compact references cross XCom. Prefix must be immutable per run.
    return list(range(len(batches)))

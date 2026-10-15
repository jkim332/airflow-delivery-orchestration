import hashlib
import json

def put_verified(s3, bucket, key, body):
    digest = hashlib.sha256(body).hexdigest()
    s3.put_object(Bucket=bucket, Key=key, Body=body,
                  Metadata={'sha256':digest})
    head = s3.head_object(Bucket=bucket, Key=key)
    if head['ContentLength'] != len(body) or head['Metadata'].get('sha256') != digest:
        raise IOError(f'verification failed: {key}')
    return {'key':key, 'bytes':len(body), 'sha256':digest}

def publish(s3, bucket, client, run_id, expected_batches, receipts):
    actual = [r['batch'] for r in receipts]
    if len(actual) != len(set(actual)) or set(actual) != set(range(expected_batches)):
        raise ValueError('missing or duplicate batch receipts')
    if not all(r.get('verified') is True for r in receipts):
        raise ValueError('unverified batch')
    key = f'{client}/{run_id}/_SUCCESS.json'
    body = json.dumps({'run_id':run_id,'batches':receipts},sort_keys=True).encode()
    return put_verified(s3,bucket,key,body)

# Production workers stream multipart copies rather than holding 500 GB in memory.
# put_verified is for small manifests/fixtures, not the bulk-transfer implementation.

from math import ceil

def plan_batches(partitions, target_size=32, max_batches=128):
    if target_size < 1 or max_batches < 1:
        raise ValueError("positive limits required")
    partitions = sorted(set(partitions))
    if not partitions:
        raise ValueError("no partitions: refusing to publish empty delivery")
    size = max(target_size, ceil(len(partitions) / max_batches))
    return [partitions[i:i+size] for i in range(0, len(partitions), size)]

def phase_clients(clients):
    ids = [c['id'] for c in clients]
    if len(ids) != len(set(ids)):
        raise ValueError('duplicate client ID')
    unknown = {c['type'] for c in clients} - {'s3','azure','gcs','snowflake'}
    if unknown:
        raise ValueError(f'unknown destinations: {unknown}')
    return ([c for c in clients if c['type'] != 'snowflake'],
            [c for c in clients if c['type'] == 'snowflake'])

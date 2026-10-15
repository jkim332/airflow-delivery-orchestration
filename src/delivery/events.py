import re
import requests

def trigger(event, base_url, token):
    # Caller authenticates event and verifies marker content before calling.
    run_id = event['run_id']
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,80}', run_id):
        raise ValueError('unsafe run identifier')
    if event['marker'].rsplit('/',1)[-1] not in {'SUCCESS','checksum.sha256'}:
        raise ValueError('not a readiness marker')
    response = requests.post(f"{base_url.rstrip('/')}/dags/client_delivery/dagRuns",
        headers={'Authorization': f'Bearer {token}'}, timeout=30,
        json={'dag_run_id': f'delivery_{run_id}', 'conf': {
            'run_id':run_id, 'business_monday':event['business_monday']}})
    if response.status_code != 409:
        response.raise_for_status()
    # 409 denotes the existing deterministic DAG run; never invent another ID.
    return response.status_code

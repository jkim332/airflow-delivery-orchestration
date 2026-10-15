import json
import logging
from .deadlines import status
logger=logging.getLogger('delivery')
def record(client, run_id, state, completed_at=None, due_at=None, error=None):
    event={'client':client,'run_id':run_id,'state':state,'error':error}
    if completed_at is not None:
        event.update(status(completed_at,due_at))
        event['completed_at']=completed_at.isoformat()
    logger.info(json.dumps(event,sort_keys=True))
    return event

def summarize(expected_clients, events):
    latest={e['client']:e for e in events}
    return {c:latest.get(c,{'state':'WAITING'}) for c in expected_clients}

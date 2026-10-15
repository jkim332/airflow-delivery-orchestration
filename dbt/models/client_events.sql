select event_id, client_id, event_timestamp, amount
from {{ source('raw', 'events') }}
qualify row_number() over (partition by event_id order by ingested_at desc) = 1

from datetime import datetime

import redis.asyncio as aioredis
import msgpack
from scheduler_service import REDIS_POOL, STREAM_NAME


async def add_payment_to_queue(payload: dict):
    correlation_id = payload['correlationId']
    amount = payload['amount']
    created_at = datetime.now().isoformat()

    r = aioredis.Redis(connection_pool=REDIS_POOL)

    packed_data = msgpack.packb((
        correlation_id,
        amount,
        created_at,
    ))

    await r.xadd(
        STREAM_NAME, 
        dict(payload=packed_data)
    )

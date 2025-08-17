import json

import redis.asyncio as aioredis
import msgpack
from scheduler_service import REDIS_POOL, STREAM_NAME


async def add_payment_to_queue(correlationId: str):
    r = aioredis.Redis(connection_pool=REDIS_POOL)
    packed_data = msgpack.packb({
        "type": "process_payment",
        "correlationId": correlationId,
    })
    await r.xadd(
        STREAM_NAME, 
        dict(payload=packed_data)
    )

import json

import redis.asyncio as aioredis
from scheduler_service import CHANNEL, REDIS_URL


async def publish_payment(correlationId: str):
    r = aioredis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
    await r.publish(
        CHANNEL, json.dumps({
            "type": "process_payment",
            "correlationId": correlationId,
        })
    )

from datetime import datetime

import msgpack
import asyncio

from rinha2025.settings import REDIS_CLIENT, QUEUE_NAME


async def add_payment_to_queue(payload: dict, processor: str = 'default'):
    correlation_id = payload['correlationId']
    amount = payload['amount']
    created_at = datetime.now().isoformat()

    asyncio.create_task(
        REDIS_CLIENT.rpush(
            f'{QUEUE_NAME}:{processor}', 
            msgpack.dumps(
                (
                    correlation_id,
                    amount,
                    created_at,
                )
            )
        )
    )

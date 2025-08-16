import asyncio
import os
import json
import logging
from datetime import datetime

import django
import redis.asyncio as aioredis

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'rinha2025.settings')
django.setup()

from rinha2025.settings import REDIS_DB, REDIS_HOST, REDIS_PORT, SCHEDULER

REDIS_URL = os.getenv("REDIS_URL", f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}")
CHANNEL = "scheduler:jobs"

logger = logging.getLogger("scheduler")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(handler)


def schedule_process_payment(correlation_id: str):
    job_id = f"process_payment_{correlation_id}"

    try:
        SCHEDULER.add_job(
            "payments.tasks:process_payment",
            args=[correlation_id],
            id=job_id,
            replace_existing=True,
            next_run_time=datetime.utcnow()
        )

        logger.info("Scheduled job %s", job_id)
    except Exception:
        logger.exception("Failed to schedule job %s", job_id)


async def redis_subscriber():
    client = aioredis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
    psub = client.pubsub()

    await psub.subscribe(CHANNEL)

    logger.info("Subscribed to Redis channel %s", CHANNEL)

    # listen loop
    try:
        async for message in psub.listen():
            if message is None:
                continue
            message_type = message.get("type")
            if message_type != "message":
                # ignore subscribe/unsubscribe messages
                continue

            raw = message.get("data")
            try:
                payload = json.loads(raw)
            except Exception:
                logger.exception("Invalid JSON from pubsub: %s", raw)
                continue

            # Basic validation
            if not isinstance(payload, dict):
                logger.warning("Invalid payload type: %r", payload)
                continue

            type = payload.get("type")
            if type == "process_payment":
                correlation_id = payload.get("correlationId")
                if not correlation_id:
                    logger.warning("process_payment message missing correlationId: %s", payload)
                    continue
                schedule_process_payment(correlation_id)

            else:
                logger.warning("Unknown message type: %s", payload)
    except asyncio.CancelledError:
        logger.info("redis_subscriber cancelled")
    except Exception:
        logger.exception("redis_subscriber crashed")
    finally:
        try:
            await psub.unsubscribe(CHANNEL)
            await client.close()
        except Exception:
            pass


async def main():
    from payments.tasks import health_check

    SCHEDULER.add_job(health_check, 'interval', seconds=10, id='health_check', replace_existing=True)
    SCHEDULER.start()
    logger.info("Scheduler started")

    sub_task = asyncio.create_task(redis_subscriber())

    try:
        await sub_task
    except asyncio.CancelledError:
        logger.info("main cancelled")
    finally:
        try:
            SCHEDULER.shutdown(wait=True)
        except Exception:
            logger.exception("Error shutting down scheduler")

if __name__ == "__main__":
    asyncio.run(main())

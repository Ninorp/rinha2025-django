import asyncio
import os
import json
import logging
from datetime import datetime

import django
import redis.asyncio as aioredis
import msgpack

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'rinha2025.settings')
django.setup()

from rinha2025.settings import SCHEDULER

REDIS_URL = os.getenv("REDIS_URL", "")
CHANNEL = "scheduler:jobs"
STREAM_NAME = 'payments:stream'
REDIS_POOL = aioredis.ConnectionPool.from_url(
    REDIS_URL, decode_responses=False,
)

GROUP_NAME = "payment_processors"
CONSUMER_NAME = "consumer-default"


QUEUE_MAXSIZE = int(os.getenv("SUBSCRIBER_QUEUE_MAXSIZE", "1000"))
WORKERS = int(os.getenv("SCHEDULER_WORKERS", "4"))

root_logger = logging.getLogger()

if root_logger.handlers:
    for handler in root_logger.handlers:
        root_logger.removeHandler(handler)

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


async def worker(queue: asyncio.Queue):
    while True:
        correlation_id = await queue.get()
        try:
            schedule_process_payment(correlation_id)
        except Exception as e:
            logger.exception("Error processing item from queue", exc_info=e)
        finally:
            queue.task_done()


async def stream_consumer(queue: asyncio.Queue):
    client = aioredis.Redis(connection_pool=REDIS_POOL)
    
    while True:
        try:
            stream_data = await client.xreadgroup(
                groupname=GROUP_NAME,
                consumername=CONSUMER_NAME,
                streams={STREAM_NAME: '>'},
                count=1,
                block=1000,
            )
            
            if not stream_data :
                await asyncio.sleep(1)
                continue

            message_id, message = stream_data[0][1][0]
            unpacked = msgpack.unpackb(message[b'payload'], raw=False)
            message_type = unpacked.get("type")
            if message_type == "process_payment":
                correlation_id = unpacked.get("correlationId")
                if not correlation_id:
                    logger.warning("process_payment message missing correlationId: %s", unpacked)
                    continue

                await queue.put(correlation_id)
                await client.xack(STREAM_NAME, GROUP_NAME, message_id)            
        except Exception as e:
            logger.exception("Error reading stream", exc_info=e)
            await asyncio.sleep(5)


async def setup_consumer_group():
    """
    Cria o grupo de consumidores se ele não existir.
    """
    client = aioredis.Redis(connection_pool=REDIS_POOL)
    try:
        # Tenta criar o grupo. O '$' significa que ele só lerá mensagens NOVAS que chegarem após a criação.
        # MKSTREAM=True cria o Stream se ele não existir ainda.
        await client.xgroup_create(STREAM_NAME, GROUP_NAME, id='$', mkstream=True)
        print(f"Grupo '{GROUP_NAME}' criado para o Stream '{STREAM_NAME}'.")
    except Exception as e:
        # Se o grupo já existe, o Redis retorna um erro. Nós podemos ignorá-lo.
        if "BUSYGROUP" in str(e):
            print(f"Grupo '{GROUP_NAME}' já existe.")
        else:
            raise e


async def main():
    from payments.tasks import health_check

    SCHEDULER.add_job(
        health_check, 'interval',
        seconds=10, id='health_check', replace_existing=True,
        misfire_grace_time=5
    )
    SCHEDULER.start()
    logger.info("Scheduler started")
    
    queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
    workers = [
        asyncio.create_task(worker(queue)) for _ in range(WORKERS)
    ]

    await setup_consumer_group()

    sub_task = asyncio.create_task(stream_consumer(queue))

    try:
        await sub_task
    except asyncio.CancelledError:
        logger.info("main cancelled")
    finally:
        try:
            SCHEDULER.shutdown(wait=True)
            REDIS_POOL.disconnect()
        except Exception:
            logger.exception("Error shutting down scheduler")
        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main())

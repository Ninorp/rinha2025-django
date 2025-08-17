import asyncio
import os
import logging

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


QUEUE_MAXSIZE = int(os.getenv("SUBSCRIBER_QUEUE_MAXSIZE", "900"))
BATCH_SIZE = int(os.getenv("SUBSCRIBER_BATCH_SIZE", "80"))
WORKERS = int(os.getenv("SCHEDULER_WORKERS", "3"))

root_logger = logging.getLogger()

if root_logger.handlers:
    for handler in root_logger.handlers:
        root_logger.removeHandler(handler)

logger = logging.getLogger("scheduler")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(handler)


async def worker(queue: asyncio.Queue):
    from payments.tasks import process_payment
    from payments.models import Payment

    while True:
        try:
            first_payload = await asyncio.wait_for(queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue

        payloads = [first_payload]
        while len(payloads) < BATCH_SIZE and not queue.empty():
            payloads.append(queue.get_nowait())

        tasks = [asyncio.create_task(process_payment(payload)) for payload in payloads]
        
        payments_to_create = []
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for res in results:
                if isinstance(res, dict):
                    payments_to_create.append(Payment(**res))
                elif isinstance(res, Exception):
                    logger.error("Error in process_payment task", exc_info=res)
        except Exception as e:
            logger.exception("Error gathering payment processing tasks", exc_info=e)

        if payments_to_create:
            try:
                await Payment.objects.abulk_create(
                    payments_to_create, ignore_conflicts=True
                )
                logger.info("Successfully bulk created %d payments.", len(payments_to_create))
            except Exception:
                logger.exception("Failed during bulk_create")
        
        for _ in range(len(payloads)):
            queue.task_done()


async def stream_consumer(queue: asyncio.Queue):
    client = aioredis.Redis(connection_pool=REDIS_POOL)
    
    while True:
        try:
            available_space = QUEUE_MAXSIZE - queue.qsize()

            if available_space <= 0:
                await asyncio.sleep(0.2)
                continue

            count = min(QUEUE_MAXSIZE, available_space)

            stream_data = await client.xreadgroup(
                groupname=GROUP_NAME,
                consumername=CONSUMER_NAME,
                streams={STREAM_NAME: '>'},
                count=count,
                block=1000,
            )
            
            if not stream_data:
                continue

            messages = stream_data[0][1]
            ids_to_ack = []

            for message_id, message in messages:
                try:
                    unpacked = msgpack.unpackb(message[b'payload'], raw=False)
                    if not isinstance(unpacked, list) or len(unpacked) != 3:
                        logger.warning("Invalid message format: %s", unpacked)
                        continue
                    
                    await queue.put(unpacked)
                    ids_to_ack.append(message_id)
                except Exception:
                    logger.exception("Failed to unpack or queue a message.")

            if ids_to_ack:
                await client.xack(STREAM_NAME, GROUP_NAME, *ids_to_ack)

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

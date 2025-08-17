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


QUEUE_MAXSIZE = int(os.getenv("SUBSCRIBER_QUEUE_MAXSIZE", "1000"))
BATCH_SIZE = int(os.getenv("SUBSCRIBER_BATCH_SIZE", "90"))
WORKERS = int(os.getenv("SCHEDULER_WORKERS", "4"))

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
        # Wait and get first payload
        try:
            first_payload = await asyncio.wait_for(queue.get(), timeout=1.0)
            payloads = [first_payload]
        except asyncio.TimeoutError:
            continue

        drain_size = min(BATCH_SIZE - 1, queue.qsize())
        for _ in range(drain_size):
            try:
                payloads.append(queue.get_nowait())
            except asyncio.QueueEmpty:
                break

        # Create asynchronous tasks (process_payment is async)
        tasks = [asyncio.create_task(process_payment(payload)) for payload in payloads]

        payments_to_create = []
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for res in results:
            if not res or not isinstance(res, dict):
                continue
            payments_to_create.append(Payment(**res))

        try:
            if payments_to_create:
                await asyncio.to_thread(Payment.objects.bulk_create, payments_to_create)
                logger.info("Successfully bulk created %d payments.", len(payments_to_create))
        except Exception:
            logger.exception("Failed during bulk_create")
        finally:
            for _ in range(len(payloads)):
                try:
                    queue.task_done()
                except Exception:
                    pass


async def stream_consumer(queue: asyncio.Queue):
    client = aioredis.Redis(connection_pool=REDIS_POOL)
    queue_size = queue.qsize()
    
    while True:
        try:
            stream_data = await client.xreadgroup(
                groupname=GROUP_NAME,
                consumername=CONSUMER_NAME,
                streams={STREAM_NAME: '>'},
                count=QUEUE_MAXSIZE - queue_size,
                block=1000,
            )
            
            if not stream_data:
                await asyncio.sleep(1)
                continue

            messages = stream_data[0][1]
            ids_to_ack = []

            for message_id, message in messages:
                unpacked = msgpack.unpackb(message[b'payload'], raw=False)

                if not unpacked[0] or not unpacked[1] or not unpacked[2]:
                    logger.warning("process_payment message missing fields: %s", unpacked)
                    continue
                
                await queue.put(unpacked)
                ids_to_ack.append(message_id)

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

import asyncio
from payments.tasks import health_check
from rinha2025.settings import SCHEDULER

from apscheduler.schedulers.asyncio import AsyncIOScheduler


def start_scheduler() -> AsyncIOScheduler:
    SCHEDULER.start()

    SCHEDULER.add_job(
        health_check,
        trigger="interval",
        seconds=5,
        id="health_check",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
    )

    return SCHEDULER


async def main():
    scheduler = start_scheduler()
    try:
        await asyncio.Future()  # roda para sempre
    except asyncio.CancelledError:
        pass
    finally:
        scheduler.shutdown(wait=True)


if __name__ == "__main__":
    asyncio.run(main())

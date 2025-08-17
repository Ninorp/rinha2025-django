import logging
import httpx

from os import getenv
from django.db import transaction
from asgiref.sync import sync_to_async
from payments.utils import add_payment_to_queue


PAYMENT_PROCESSOR_URL_DEFAULT = getenv("PAYMENT_PROCESSOR_URL_DEFAULT")
PAYMENT_PROCESSOR_URL_FALLBACK = getenv("PAYMENT_PROCESSOR_URL_FALLBACK")

HEALTH_RESULTS = {}

logger = logging.getLogger('payments_tasks')
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler("payments.log")
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)

@sync_to_async
def _get_payment_locked(correlation_id: str):
    from payments.models import Payment

    with transaction.atomic():
        return Payment.objects.select_for_update(
            of=("self",)
        ).get(correlationId=correlation_id)


@sync_to_async
def _mark_completed(payment, url):
    payment.mark_as_completed(url)


async def process_payment(payment_id: str):
    payment = await _get_payment_locked(payment_id)

    async def _process(url: str) -> bool:
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                logger.debug("Calling %s", url)
                payments_url = url + '/payments'
                resp = await client.post(
                    payments_url,
                    json={
                        "correlationId": payment_id,
                        "amount": float(payment.amount),
                        "requestedAt": payment.created_at.isoformat(),
                    },
                )
                logger.debug("Response: %s", resp.json())
            return resp.status_code == 200
        except Exception:
            return False

    url = None
    for _url, result in HEALTH_RESULTS.items():
        if not result.get("failing"):
            url = _url
            break

    logger.debug("Selected url: %s", url)

    if url and await _process(url):
        await _mark_completed(payment, url)
        return

    logger.debug('Rescheduling payment %s', payment_id)
    await add_payment_to_queue(payment_id)


async def health_check():
    async def _check(url: str):
        try:
            async with httpx.AsyncClient(timeout=2) as client:
                resp = await client.get(f"{url}/payments/service-health")
            if resp.status_code == 200:
                HEALTH_RESULTS[url] = resp.json()
                return
        except Exception:
            pass
        HEALTH_RESULTS[url] = {"failing": True, "minRequestTime": 999999}

    await _check(PAYMENT_PROCESSOR_URL_DEFAULT)
    await _check(PAYMENT_PROCESSOR_URL_FALLBACK)

    logger.info("Health results: %s", HEALTH_RESULTS)

import logging
import httpx

from os import getenv

from payments.utils import add_payment_to_queue


PAYMENT_PROCESSOR_URL_DEFAULT = getenv("PAYMENT_PROCESSOR_URL_DEFAULT")
PAYMENT_PROCESSOR_URL_FALLBACK = getenv("PAYMENT_PROCESSOR_URL_FALLBACK")
HEALTH_RESULTS = {}

TIMEOUT=httpx.Timeout(5, connect=1)

logger = logging.getLogger('payments_tasks')
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler("payments.log")
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)


async def process_payment(payload: list, processor: str):
    correlation_id = payload[0]
    amount = payload[1]
    created_at = payload[2]

    async def _process(url: str) -> bool:
        try:
            async with httpx.AsyncClient(timeout=TIMEOUT) as client:
                payments_url = url + '/payments'
                resp = await client.post(
                    payments_url,
                    json={
                        "correlationId": correlation_id,
                        "amount": float(amount),
                        "requestedAt": created_at,
                    },
                    headers={
                        "Content-Type": "application/json",
                    }
                )
            return resp.status_code == 200
        except Exception:
            return False

    url = PAYMENT_PROCESSOR_URL_DEFAULT if processor == 'default' else PAYMENT_PROCESSOR_URL_FALLBACK
    logger.debug("Processing payment with: %s", url)

    if url and await _process(url):
        return dict(
            correlationId=correlation_id,
            amount=amount,
            created_at=created_at,
            status='completed',
            gatewayIdentifier=url,
        )

    logger.debug('Failed to process payment: %s', correlation_id)
    await add_payment_to_queue({
        "correlationId": correlation_id,
        "amount": amount,
        "created_at": created_at
    }, processor='default' if processor == 'fallback' else 'fallback')
    return None
    


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


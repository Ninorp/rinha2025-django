import httpx

from time import sleep
from os import getenv
from huey import crontab
from huey.contrib.djhuey import task, periodic_task

from payments.models import Payment


PAYMENT_PROCESSOR_URL_DEFAULT = getenv("PAYMENT_PROCESSOR_URL_DEFAULT")
PAYMENT_PROCESSOR_URL_FALLBACK = getenv("PAYMENT_PROCESSOR_URL_FALLBACK")

HEALTH_RESULTS = {}


@task(priority=1, retry=50, retry_delay=5)
def process_payment(payment_id):
    payment = Payment.objects.get(correlation_id=payment_id)
    
    def _process(url):
        try:
            response = httpx.post(url, json={
                "correlationId": payment_id,
                "amount": payment.amount,
                "requestedAt": payment.created_at.isoformat(),
            })
            return response.status_code == 200
        except Exception:
            return False
        
    url = None
    for _url, result in HEALTH_RESULTS.items():
        if not result.get("failing"):
            url = _url
            break
    
    if url:
        ok = _process(url)
        if ok:
            payment.mark_as_completed()
            return
    
    raise Exception("Payment processing failed")


@periodic_task(lambda _: True, priority=0)
def health_check():
    sleep(5)
    def _check(url):
        try:
            response = httpx.get(f'{url}/payments/service-health', timeout=2)
            if response.status_code == 200:
                HEALTH_RESULTS[url] = response.json()
        except Exception:
            HEALTH_RESULTS[url] = {
                "failing": True,
                "minRequestTime": 999999
            }

    _check(PAYMENT_PROCESSOR_URL_DEFAULT)
    _check(PAYMENT_PROCESSOR_URL_FALLBACK)

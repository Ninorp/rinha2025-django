from django.http import HttpResponse, HttpResponseBadRequest
from ninja import Router

from payments.models import Payment
from payments.schemas import PaymentSchema
from payments.tasks import process_payment

from rinha2025.settings import SCHEDULER


payments_router = Router()

@payments_router.post('/')
def create_payment(request, payload: PaymentSchema):
    if payload.amount <= 0:
        return HttpResponseBadRequest()

    Payment.objects.create(
        **payload.dict()
    )

    SCHEDULER.add_job(
        process_payment,
        args=[payload.correlation_id],
        id=f"process_payment_{payload.correlation_id}",
        replace_existing=True,
    )

    return HttpResponse()

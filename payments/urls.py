from django.http import HttpResponse, HttpResponseBadRequest
from django.db import IntegrityError
from ninja import Router

from payments.models import Payment
from payments.schemas import PaymentSchema
from payments.tasks import process_payment

from rinha2025.settings import SCHEDULER


payments_router = Router()


@payments_router.post('')
def create_payment(request, payload: PaymentSchema):
    if payload.amount <= 0:
        return HttpResponseBadRequest()

    try:
        Payment.objects.create(
            **payload.dict()
        )
    except IntegrityError:
        return HttpResponseBadRequest("Integrity error occurred while creating the payment.")

    SCHEDULER.add_job(
        process_payment,
        args=[payload.correlationId],
        id=f"process_payment_{payload.correlationId}",
        replace_existing=True,
    )

    return HttpResponse()

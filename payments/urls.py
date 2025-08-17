from django.http import HttpResponse, HttpResponseBadRequest
from django.db import IntegrityError
from ninja import Router

from payments.models import Payment
from payments.schemas import PaymentSchema
from payments.utils import add_payment_to_queue


payments_router = Router()


@payments_router.post('')
async def create_payment(request, payload: PaymentSchema):
    if payload.amount <= 0:
        return HttpResponseBadRequest()

    try:
        await Payment.objects.acreate(
            **payload.dict()
        )
    except IntegrityError:
        return HttpResponseBadRequest("Integrity error occurred while creating the payment.")

    await add_payment_to_queue(payload.correlationId)
    return HttpResponse()

from django.http import HttpResponse, HttpResponseBadRequest
from ninja import Router

from payments.schemas import PaymentSchema
from payments.utils import add_payment_to_queue


payments_router = Router()


@payments_router.post('')
async def create_payment(request, payload: PaymentSchema):
    if payload.amount <= 0:
        return HttpResponseBadRequest()

    payload_dict = payload.dict()
    await add_payment_to_queue(payload_dict)

    return HttpResponse()

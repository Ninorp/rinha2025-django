from django.urls import path
from django.http import HttpResponse, HttpResponseBadRequest
from ninja import Router

from payments.models import Payment
from payments.schemas import PaymentSchema


payments_router = Router()

@payments_router.post('/')
def create_payment(request, payload: PaymentSchema):
    if payload.amount <= 0:
        return HttpResponseBadRequest()

    Payment.objects.create(
        **payload.dict()
    )
    return HttpResponse()

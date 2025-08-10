from datetime import datetime
from decimal import Decimal
from django.db.models import Sum, Count
from ninja import Query, Router

from payments.models import Payment
from payments.tasks import (
    PAYMENT_PROCESSOR_URL_DEFAULT,
    PAYMENT_PROCESSOR_URL_FALLBACK
)


payments_summary_router = Router()


@payments_summary_router.get('/payments-summary')
def payments_summary(
    request,
    date_from: datetime = Query(..., alias="from"),
    date_to:   datetime = Query(..., alias="to")
):
    payments = Payment.objects.filter(
        status='completed',
    ).order_by('created_at')

    if date_from is not None:
        payments = payments.filter(created_at__gte=date_from)
    if date_to is not None:
        payments = payments.filter(created_at__lte=date_to)

    default_summary = payments.filter(
        gatewayIdentifier=PAYMENT_PROCESSOR_URL_DEFAULT
    ).aggregate(
        totalAmount=Sum('amount'),
        totalRequests=Count('correlationId'),
    )

    fallback_summary = payments.filter(
        gatewayIdentifier=PAYMENT_PROCESSOR_URL_FALLBACK
    ).aggregate(
        totalAmount=Sum('amount'),
        totalRequests=Count('correlationId'),
    )

    return {
        "default": normalize(default_summary),
        "fallback": normalize(fallback_summary),
    }


def normalize(summary):
    return {
        "totalAmount": summary.get("totalAmount") or Decimal("0"),
        "totalRequests": summary.get("totalRequests") or 0,
    }

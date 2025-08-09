from ninja import Schema


class PaymentSchema(Schema):
    correlationId: str
    amount: float

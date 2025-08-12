from django.db import models


class Payment(models.Model):
    correlationId = models.CharField(max_length=255, primary_key=True)
    amount = models.DecimalField(max_digits=10, decimal_places=2)
    status = models.CharField(max_length=20, default='pending')
    gatewayIdentifier = models.CharField(max_length=255, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.correlationId

    class Meta:
        ordering = ['-created_at']
        db_table = 'payments_payment'
        indexes = [
            models.Index(
                fields=['created_at', 'gatewayIdentifier', 'status', 'amount'],
                name='payment_index'
            )
        ]

    def mark_as_completed(self, gateway_url: str = None):
        self.status = 'completed'
        self.gatewayIdentifier = gateway_url
        self.save(update_fields=['status', 'gatewayIdentifier'])

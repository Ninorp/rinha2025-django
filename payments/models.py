from django.db import models


class Payment(models.Model):
    correlationId = models.CharField(max_length=255, primary_key=True)
    amount = models.DecimalField(max_digits=10, decimal_places=2)
    status = models.CharField(max_length=20, default='pending')
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.correlation_id

    class Meta:
        ordering = ['-created_at']
        indexes = [
            models.Index(
                fields=['created_at', 'status', 'amount'],
                name='payment_index'
            )
        ]

    def mark_as_completed(self):
        self.status = 'completed'
        self.save(update_fields=['status'])

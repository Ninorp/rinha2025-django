"""
URL configuration for rinha2025 project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.urls import path
from ninja import NinjaAPI

from payments.urls import payments_router
from payments_summary.urls import payments_summary_router

# Create a NinjaAPI instance and attach the payments router
api = NinjaAPI()
api.add_router("/payments", payments_router)
api.add_router("/payments-summary", payments_summary_router)

urlpatterns = [
    path("", api.urls),
]

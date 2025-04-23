from django.urls import path
from .views import FileDownloadView

urlpatterns = [
    path('download/<str:filename>/', FileDownloadView.as_view()),
]

from django.urls import path

from main.views import get_data

app_name = 'main'


urlpatterns = [
    path('', get_data, name='test')
]

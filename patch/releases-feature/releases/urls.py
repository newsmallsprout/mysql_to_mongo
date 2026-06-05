from django.urls import path

from . import views

urlpatterns = [
    path('webhook/ecr', views.ecr_webhook, name='ecr_webhook'),
    path('apps', views.app_list, name='release_app_list'),
    path('apps/<int:app_id>/history', views.app_history, name='release_app_history'),
    path('apps/<int:app_id>/rollback', views.app_rollback, name='release_app_rollback'),
    path('config', views.config_list, name='release_config_list'),
    path('config/<int:config_id>', views.config_detail, name='release_config_detail'),
]

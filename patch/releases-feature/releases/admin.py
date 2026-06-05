from django.contrib import admin

from .models import ReleaseAppConfig, ReleaseRecord


@admin.register(ReleaseAppConfig)
class ReleaseAppConfigAdmin(admin.ModelAdmin):
    list_display = ['display_name', 'ecr_repository', 'argo_app_name', 'current_tag', 'enabled']
    search_fields = ['display_name', 'ecr_repository', 'argo_app_name']


@admin.register(ReleaseRecord)
class ReleaseRecordAdmin(admin.ModelAdmin):
    list_display = ['app_config', 'image_tag', 'source', 'argo_success', 'is_current', 'created_at']
    list_filter = ['source', 'argo_success', 'is_current']

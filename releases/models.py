from django.conf import settings
from django.db import models


class ReleaseAppConfig(models.Model):
    display_name = models.CharField(max_length=255)
    ecr_repository = models.CharField(max_length=255, unique=True, help_text="ECR repository-name from EventBridge event")
    argo_app_name = models.CharField(max_length=255, help_text="Argo CD Application name")
    argo_project = models.CharField(max_length=255, blank=True, default='')
    repo_url = models.CharField(max_length=512, help_text="OCI repo URL, e.g. 197461532043.dkr.ecr.ap-northeast-1.amazonaws.com")
    chart_path = models.CharField(max_length=512, help_text="Helm chart path, e.g. helm-main/exchange-activity")
    enabled = models.BooleanField(default=True)
    current_tag = models.CharField(max_length=255, blank=True, default='')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['display_name']

    def __str__(self):
        return self.display_name

    @property
    def chart_display(self):
        if self.current_tag:
            return f"{self.chart_path}:{self.current_tag}"
        return self.chart_path


class ReleaseRecord(models.Model):
    SOURCE_CHOICES = [
        ('webhook', 'Webhook'),
        ('rollback', 'Rollback'),
    ]

    app_config = models.ForeignKey(ReleaseAppConfig, on_delete=models.CASCADE, related_name='records')
    image_tag = models.CharField(max_length=255)
    image_digest = models.CharField(max_length=255, blank=True, default='')
    source = models.CharField(max_length=20, choices=SOURCE_CHOICES)
    argo_success = models.BooleanField(default=False)
    argo_error = models.TextField(blank=True, default='')
    raw_event = models.JSONField(null=True, blank=True)
    operator = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name='release_records',
    )
    is_current = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.app_config.display_name} @ {self.image_tag}"

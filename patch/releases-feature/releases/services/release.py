import logging

from django.db import transaction

from ..models import ReleaseAppConfig, ReleaseRecord
from .argocd import ArgoCDClient, ArgoCDError
from .mapping import resolve_app_config

logger = logging.getLogger(__name__)


def _set_current_record(app_config, record):
    ReleaseRecord.objects.filter(app_config=app_config, is_current=True).update(is_current=False)
    record.is_current = True
    record.save(update_fields=['is_current'])
    app_config.current_tag = record.image_tag
    app_config.save(update_fields=['current_tag', 'updated_at'])


def apply_release(app_config, image_tag, source, raw_event=None, operator=None, image_digest=''):
    argo_client = ArgoCDClient()
    argo_success = False
    argo_error = ''

    try:
        if not app_config.enabled:
            raise ArgoCDError(f"Application '{app_config.display_name}' is disabled")

        argo_client.update_target_revision(
            app_config.argo_app_name,
            app_config.argo_project,
            image_tag,
        )
        argo_success = True
    except ArgoCDError as exc:
        argo_error = str(exc)
        logger.error("Argo CD update failed for %s: %s", app_config.argo_app_name, argo_error)

    with transaction.atomic():
        record = ReleaseRecord.objects.create(
            app_config=app_config,
            image_tag=image_tag,
            image_digest=image_digest or '',
            source=source,
            argo_success=argo_success,
            argo_error=argo_error,
            raw_event=raw_event,
            operator=operator,
            is_current=False,
        )
        if argo_success:
            _set_current_record(app_config, record)

    return record


def handle_ecr_webhook(detail):
    repository = detail.get('repository-name') or detail.get('repositoryName', '')
    image_tag = detail.get('image-tag') or detail.get('imageTag', '')
    image_digest = detail.get('image-digest') or detail.get('imageDigest', '')
    action_type = detail.get('action-type') or detail.get('actionType', '')
    result = detail.get('result', '')

    if action_type and action_type != 'PUSH':
        return None, 'ignored: not a PUSH action'
    if result and result != 'SUCCESS':
        return None, 'ignored: result is not SUCCESS'
    if not repository or not image_tag:
        return None, 'missing repository-name or image-tag'

    app_config, auto_created = resolve_app_config(repository)
    if app_config is None:
        return None, auto_created

    record = apply_release(
        app_config,
        image_tag=image_tag,
        source='webhook',
        raw_event=detail,
        image_digest=image_digest,
    )
    message = 'ok (auto-created mapping)' if auto_created else 'ok'
    return record, message


def rollback_release(app_config, image_tag, operator=None):
    return apply_release(
        app_config,
        image_tag=image_tag,
        source='rollback',
        operator=operator,
    )

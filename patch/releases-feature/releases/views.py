import logging
import os

from django.shortcuts import get_object_or_404
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework.response import Response

from .models import ReleaseAppConfig, ReleaseRecord
from .services.argocd import ArgoCDClient
from .services.release import handle_ecr_webhook, rollback_release

logger = logging.getLogger(__name__)


def _serialize_app_config(app):
    return {
        'id': app.id,
        'display_name': app.display_name,
        'ecr_repository': app.ecr_repository,
        'argo_app_name': app.argo_app_name,
        'argo_project': app.argo_project,
        'repo_url': app.repo_url,
        'chart_path': app.chart_path,
        'enabled': app.enabled,
        'current_tag': app.current_tag,
        'chart_display': app.chart_display,
        'updated_at': app.updated_at.isoformat() if app.updated_at else None,
    }


def _serialize_record(record):
    return {
        'id': record.id,
        'image_tag': record.image_tag,
        'image_digest': record.image_digest,
        'source': record.source,
        'argo_success': record.argo_success,
        'argo_error': record.argo_error,
        'is_current': record.is_current,
        'operator': record.operator.username if record.operator else None,
        'created_at': record.created_at.isoformat(),
    }


def _check_webhook_token(request):
    expected = os.environ.get('ECR_WEBHOOK_TOKEN', '')
    if not expected:
        return False

    auth_header = request.headers.get('Authorization', '')
    if auth_header == f'Bearer {expected}':
        return True
    if auth_header == expected:
        return True

    token_header = request.headers.get('X-Webhook-Token', '')
    if token_header == expected:
        return True

    return False


@api_view(['POST'])
@permission_classes([AllowAny])
def ecr_webhook(request):
    if not _check_webhook_token(request):
        return Response({'detail': 'Unauthorized'}, status=401)

    payload = request.data
    detail = payload.get('detail') if isinstance(payload, dict) else None
    if not detail and isinstance(payload, dict):
        detail = payload

    if not isinstance(detail, dict):
        return Response({'status': 'ignored', 'reason': 'invalid payload'}, status=400)

    try:
        record, message = handle_ecr_webhook(detail)
    except Exception as exc:
        logger.exception('ECR webhook processing failed')
        return Response({'status': 'error', 'detail': str(exc)}, status=500)

    if record is None:
        return Response({'status': 'ignored', 'reason': message})

    return Response({
        'status': 'ok' if record.argo_success else 'partial',
        'message': message,
        'record': _serialize_record(record),
        'app': _serialize_app_config(record.app_config),
    })


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def app_list(request):
    apps = ReleaseAppConfig.objects.all()
    argo = ArgoCDClient()
    data = []
    for app in apps:
        item = _serialize_app_config(app)
        if argo.configured:
            try:
                argo_app = argo.get_application(app.argo_app_name, app.argo_project)
                source = (argo_app.get('spec') or {}).get('source') or {}
                item['argo_live_revision'] = source.get('targetRevision', '')
                sync_status = (argo_app.get('status') or {}).get('sync') or {}
                item['argo_sync_status'] = sync_status.get('status', '')
                health = (argo_app.get('status') or {}).get('health') or {}
                item['argo_health'] = health.get('status', '')
            except Exception as exc:
                item['argo_live_revision'] = ''
                item['argo_sync_status'] = ''
                item['argo_health'] = ''
                item['argo_error'] = str(exc)
        data.append(item)

    return Response({
        'apps': data,
        'argocd_configured': argo.configured,
    })


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def app_history(request, app_id):
    app = get_object_or_404(ReleaseAppConfig, id=app_id)
    records = app.records.all()[:100]
    return Response({
        'app': _serialize_app_config(app),
        'records': [_serialize_record(r) for r in records],
    })


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def app_rollback(request, app_id):
    app = get_object_or_404(ReleaseAppConfig, id=app_id)
    image_tag = (request.data.get('image_tag') or '').strip()
    if not image_tag:
        return Response({'detail': 'image_tag is required'}, status=400)

    record = rollback_release(app, image_tag, operator=request.user)
    return Response({
        'status': 'ok' if record.argo_success else 'partial',
        'record': _serialize_record(record),
        'app': _serialize_app_config(record.app_config),
    })


@api_view(['GET', 'POST'])
@permission_classes([IsAuthenticated])
def config_list(request):
    if request.method == 'GET':
        apps = ReleaseAppConfig.objects.all()
        return Response({'configs': [_serialize_app_config(a) for a in apps]})

    data = request.data
    required = ['display_name', 'ecr_repository', 'argo_app_name', 'repo_url', 'chart_path']
    for field in required:
        if not (data.get(field) or '').strip():
            return Response({'detail': f'{field} is required'}, status=400)

    app = ReleaseAppConfig.objects.create(
        display_name=data['display_name'].strip(),
        ecr_repository=data['ecr_repository'].strip(),
        argo_app_name=data['argo_app_name'].strip(),
        argo_project=(data.get('argo_project') or '').strip(),
        repo_url=data['repo_url'].strip(),
        chart_path=data['chart_path'].strip(),
        enabled=data.get('enabled', True),
    )
    return Response({'config': _serialize_app_config(app)}, status=201)


@api_view(['PUT', 'DELETE'])
@permission_classes([IsAuthenticated])
def config_detail(request, config_id):
    app = get_object_or_404(ReleaseAppConfig, id=config_id)

    if request.method == 'DELETE':
        app.delete()
        return Response({'status': 'ok'})

    data = request.data
    for field in ['display_name', 'ecr_repository', 'argo_app_name', 'argo_project', 'repo_url', 'chart_path']:
        if field in data:
            setattr(app, field, (data[field] or '').strip())
    if 'enabled' in data:
        app.enabled = bool(data['enabled'])
    app.save()
    return Response({'config': _serialize_app_config(app)})

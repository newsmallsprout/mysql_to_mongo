import logging
import os

from ..models import ReleaseAppConfig

logger = logging.getLogger(__name__)


def _env_bool(name, default=True):
    val = os.environ.get(name)
    if val is None:
        return default
    return val.lower() in ('1', 'true', 'yes', 'on')


def derive_argo_app_name(ecr_repository: str) -> str:
    mode = (os.environ.get('RELEASE_APP_NAME_MODE') or 'upper').lower()
    if mode == 'upper':
        return ecr_repository.upper()
    if mode == 'same':
        return ecr_repository
    return ecr_repository


def derive_chart_path(ecr_repository: str) -> str:
    prefix = (os.environ.get('ARGOCD_CHART_PREFIX') or 'helm-main').strip().strip('/')
    return f"{prefix}/{ecr_repository}"


def resolve_app_config(ecr_repository: str):
    """
    Return (app_config, auto_created) or (None, error_message).
    """
    existing = ReleaseAppConfig.objects.filter(ecr_repository=ecr_repository).first()
    if existing:
        if not existing.enabled:
            return None, f'mapping disabled for repository: {ecr_repository}'
        return existing, False

    if not _env_bool('RELEASE_AUTO_CREATE_MAPPING', True):
        return None, f'no mapping for repository: {ecr_repository}'

    repo_url = (os.environ.get('ARGOCD_DEFAULT_REPO_URL') or '').strip()
    if not repo_url:
        return None, f'no mapping for repository: {ecr_repository} (set ARGOCD_DEFAULT_REPO_URL)'

    argo_app_name = derive_argo_app_name(ecr_repository)
    chart_path = derive_chart_path(ecr_repository)
    project = (os.environ.get('ARGOCD_DEFAULT_PROJECT') or '').strip()

    app_config = ReleaseAppConfig.objects.create(
        display_name=argo_app_name,
        ecr_repository=ecr_repository,
        argo_app_name=argo_app_name,
        argo_project=project,
        repo_url=repo_url,
        chart_path=chart_path,
        enabled=True,
    )
    logger.info(
        "Auto-created release mapping: ecr=%s argo=%s chart=%s",
        ecr_repository,
        argo_app_name,
        chart_path,
    )
    return app_config, True

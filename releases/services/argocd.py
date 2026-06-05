import logging
import os

import requests

logger = logging.getLogger(__name__)


class ArgoCDError(Exception):
    pass


class ArgoCDClient:
    def __init__(self):
        self.base_url = (os.environ.get('ARGOCD_URL') or '').rstrip('/')
        self.username = os.environ.get('ARGOCD_USERNAME', '')
        self.password = os.environ.get('ARGOCD_PASSWORD', '')

    @property
    def configured(self):
        return bool(self.base_url and self.username and self.password)

    def _auth(self):
        return (self.username, self.password)

    def _params(self, project):
        return {'project': project} if project else None

    def get_application(self, app_name, project=''):
        if not self.configured:
            raise ArgoCDError('Argo CD is not configured (ARGOCD_URL/USERNAME/PASSWORD)')

        url = f"{self.base_url}/api/v1/applications/{app_name}"
        resp = requests.get(url, auth=self._auth(), params=self._params(project), timeout=30)
        if resp.status_code == 404:
            raise ArgoCDError(f"Application '{app_name}' not found in Argo CD")
        if not resp.ok:
            raise ArgoCDError(f"Failed to get application: {resp.status_code} {resp.text[:500]}")
        return resp.json()

    def update_target_revision(self, app_name, project, target_revision):
        app = self.get_application(app_name, project)
        spec = app.get('spec') or {}
        source = spec.get('source') or {}
        old_revision = source.get('targetRevision', '')
        source['targetRevision'] = target_revision
        spec['source'] = source

        payload = {
            'metadata': app.get('metadata', {}),
            'spec': spec,
        }

        url = f"{self.base_url}/api/v1/applications/{app_name}"
        resp = requests.put(url, json=payload, auth=self._auth(), params=self._params(project), timeout=30)
        if not resp.ok:
            raise ArgoCDError(f"Failed to update targetRevision: {resp.status_code} {resp.text[:500]}")

        logger.info(
            "Updated Argo CD app %s targetRevision: %s -> %s",
            app_name,
            old_revision,
            target_revision,
        )
        return {'old_revision': old_revision, 'new_revision': target_revision}

#!/bin/bash
# Apply release-management feature onto production codebase (v1.0.7 base).
# Usage: cd /root/ops-v1.0.7-code && bash patch/apply-patch.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(pwd)"

if [ ! -f "$ROOT/manage.py" ] || [ ! -d "$ROOT/shark_platform" ]; then
  echo "Error: run from project root (manage.py must exist)"
  exit 1
fi

echo "==> Copy new files"
cp -r "$SCRIPT_DIR/releases" "$ROOT/"
mkdir -p "$ROOT/frontend/src/api" "$ROOT/frontend/src/views/Releases"
cp "$SCRIPT_DIR/frontend/src/api/releases.ts" "$ROOT/frontend/src/api/"
cp "$SCRIPT_DIR/frontend/src/views/Releases/Index.vue" "$ROOT/frontend/src/views/Releases/"

patch_file() {
  local file="$1"
  local needle="$2"
  local line="$3"
  if grep -qF "$needle" "$file"; then
    return 0
  fi
  if grep -qF "$line" "$file"; then
    return 0
  fi
  local anchor
  for anchor in "${@:4}"; do
    if grep -qF "$anchor" "$file"; then
      sed -i "/$(echo "$anchor" | sed 's/[\/&]/\\&/g')/a\\${line}" "$file"
      return 0
    fi
  done
  echo "ERROR: could not patch $file (no anchor found)"
  return 1
}

echo "==> Patch shark_platform/settings.py"
patch_file "$ROOT/shark_platform/settings.py" "'releases'," "    'releases'," \
  "'traffic'," "'db_manager'," "'ai_ops'," "'schedules',"

grep -q "'releases':" "$ROOT/shark_platform/settings.py" || \
  patch_file "$ROOT/shark_platform/settings.py" "'releases':" \
  "        'releases': {\\
            'handlers': ['console'],\\
            'level': 'INFO',\\
            'propagate': True,\\
        }," \
  "'inspection': {" 

grep -q "ARGOCD_URL" "$ROOT/shark_platform/settings.py" || cat >> "$ROOT/shark_platform/settings.py" <<'EOF'

# Argo CD release management
ARGOCD_URL = os.environ.get('ARGOCD_URL', '')
ARGOCD_USERNAME = os.environ.get('ARGOCD_USERNAME', '')
ARGOCD_PASSWORD = os.environ.get('ARGOCD_PASSWORD', '')
ECR_WEBHOOK_TOKEN = os.environ.get('ECR_WEBHOOK_TOKEN', '')
ARGOCD_DEFAULT_PROJECT = os.environ.get('ARGOCD_DEFAULT_PROJECT', '')
ARGOCD_DEFAULT_REPO_URL = os.environ.get('ARGOCD_DEFAULT_REPO_URL', '')
ARGOCD_CHART_PREFIX = os.environ.get('ARGOCD_CHART_PREFIX', 'helm-main')
RELEASE_APP_NAME_MODE = os.environ.get('RELEASE_APP_NAME_MODE', 'upper')
RELEASE_AUTO_CREATE_MAPPING = os.environ.get('RELEASE_AUTO_CREATE_MAPPING', 'true')
EOF

grep -q "ARGOCD_DEFAULT_REPO_URL" "$ROOT/shark_platform/settings.py" || cat >> "$ROOT/shark_platform/settings.py" <<'EOF'
ARGOCD_DEFAULT_PROJECT = os.environ.get('ARGOCD_DEFAULT_PROJECT', '')
ARGOCD_DEFAULT_REPO_URL = os.environ.get('ARGOCD_DEFAULT_REPO_URL', '')
ARGOCD_CHART_PREFIX = os.environ.get('ARGOCD_CHART_PREFIX', 'helm-main')
RELEASE_APP_NAME_MODE = os.environ.get('RELEASE_APP_NAME_MODE', 'upper')
RELEASE_AUTO_CREATE_MAPPING = os.environ.get('RELEASE_AUTO_CREATE_MAPPING', 'true')
EOF

echo "==> Patch shark_platform/urls.py"
patch_file "$ROOT/shark_platform/urls.py" "releases.urls" \
  "    path('api/releases/', include('releases.urls'))," \
  "traffic.urls" "db_manager.urls" "ai_ops.urls" "schedules.urls"

echo "==> Patch frontend router"
bash "$SCRIPT_DIR/fix-router.sh" "$ROOT"

echo "==> Patch AppSidebar.vue"
grep -q 'index="/releases"' "$ROOT/frontend/src/components/Layout/AppSidebar.vue" || \
  sed -i '/index="\/permissions"/i\
        <el-menu-item index="/releases">\
          <el-icon><Promotion /></el-icon>\
          <template #title>Release Mgmt</template>\
        </el-menu-item>' "$ROOT/frontend/src/components/Layout/AppSidebar.vue"

grep -q 'Promotion' "$ROOT/frontend/src/components/Layout/AppSidebar.vue" || \
  sed -i 's/Cpu } from/Cpu, Promotion } from/' "$ROOT/frontend/src/components/Layout/AppSidebar.vue"

echo "==> Verify"
grep -q "'releases'," "$ROOT/shark_platform/settings.py" || { echo "FAIL: settings INSTALLED_APPS"; exit 1; }
grep -q "releases.urls" "$ROOT/shark_platform/urls.py" || { echo "FAIL: urls.py"; exit 1; }
grep -q "path: 'releases'" "$ROOT/frontend/src/router/index.ts" || { echo "FAIL: router"; exit 1; }
grep -q 'index="/releases"' "$ROOT/frontend/src/components/Layout/AppSidebar.vue" || { echo "FAIL: sidebar"; exit 1; }

echo "==> Done. Build v1.0.8 with your own script when ready."

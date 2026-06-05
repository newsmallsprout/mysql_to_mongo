#!/bin/bash
# Apply release-management feature onto production codebase (v1.0.7 base).
# Usage: cd /path/to/shark-Platform && bash /path/to/releases-feature/apply-patch.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(pwd)"

if [ ! -f "$ROOT/manage.py" ] || [ ! -d "$ROOT/shark_platform" ]; then
  echo "Error: run this from shark-Platform root (manage.py must exist)"
  exit 1
fi

echo "==> Copy new files"
cp -r "$SCRIPT_DIR/releases" "$ROOT/"
mkdir -p "$ROOT/frontend/src/views/Releases"
cp "$SCRIPT_DIR/frontend/src/api/releases.ts" "$ROOT/frontend/src/api/"
cp "$SCRIPT_DIR/frontend/src/views/Releases/Index.vue" "$ROOT/frontend/src/views/Releases/"

echo "==> Patch shark_platform/settings.py"
grep -q "'releases'," "$ROOT/shark_platform/settings.py" || \
  sed -i "/'traffic',/a\\    'releases'," "$ROOT/shark_platform/settings.py"

grep -q "'releases':" "$ROOT/shark_platform/settings.py" || \
  sed -i "/'inspection': {/,/},/{
    /},/a\\
        'releases': {\\
            'handlers': ['console'],\\
            'level': 'INFO',\\
            'propagate': True,\\
        },
  }" "$ROOT/shark_platform/settings.py"

grep -q "ARGOCD_URL" "$ROOT/shark_platform/settings.py" || cat >> "$ROOT/shark_platform/settings.py" <<'EOF'

# Argo CD release management
ARGOCD_URL = os.environ.get('ARGOCD_URL', '')
ARGOCD_USERNAME = os.environ.get('ARGOCD_USERNAME', '')
ARGOCD_PASSWORD = os.environ.get('ARGOCD_PASSWORD', '')
ECR_WEBHOOK_TOKEN = os.environ.get('ECR_WEBHOOK_TOKEN', '')
EOF

echo "==> Patch shark_platform/urls.py"
grep -q "releases.urls" "$ROOT/shark_platform/urls.py" || \
  sed -i "/traffic.urls/a\\    path('api/releases/', include('releases.urls'))," "$ROOT/shark_platform/urls.py"

echo "==> Patch frontend router"
grep -q "path: 'releases'" "$ROOT/frontend/src/router/index.ts" || \
  sed -i "/path: 'permissions'/i\\
      {\\
        path: 'releases',\\
        name: 'Releases',\\
        component: () => import('@/views/Releases/Index.vue'),\\
        meta: { title: 'Releases', icon: 'Promotion' }\\
      }," "$ROOT/frontend/src/router/index.ts"

echo "==> Patch AppSidebar.vue"
grep -q 'index="/releases"' "$ROOT/frontend/src/components/Layout/AppSidebar.vue" || \
  sed -i '/index="\/permissions"/i\
        <el-menu-item index="/releases">\
          <el-icon><Promotion /></el-icon>\
          <template #title>Release Mgmt</template>\
        </el-menu-item>' "$ROOT/frontend/src/components/Layout/AppSidebar.vue"

grep -q 'Promotion' "$ROOT/frontend/src/components/Layout/AppSidebar.vue" || \
  sed -i 's/Cpu } from/Cpu, Promotion } from/' "$ROOT/frontend/src/components/Layout/AppSidebar.vue"

echo "==> Done. Release feature patched into codebase."
echo "    Build v1.0.8 image with your own script when ready."

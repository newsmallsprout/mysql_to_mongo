#!/usr/bin/env python3
"""Fix router/index.ts after a broken sed patch."""
from pathlib import Path

path = Path("frontend/src/router/index.ts")
if not path.exists():
    raise SystemExit("frontend/src/router/index.ts not found — run from project root")

content = path.read_text()

# Remove duplicate opening brace before releases
content = content.replace(
    "      {\n      {\n        path: 'releases',",
    "      {\n        path: 'releases',",
)

# Restore missing { before permissions route
content = content.replace(
    "      },\n        path: 'permissions',",
    "      },\n      {\n        path: 'permissions',",
)

releases_block = """
      {
        path: 'releases',
        name: 'Releases',
        component: () => import('@/views/Releases/Index.vue'),
        meta: { title: 'Releases', icon: 'Promotion' }
      },"""

if "path: 'releases'" not in content:
    marker = "meta: { title: 'Deploy', icon: 'Upload', viewPerm: 'view_deploy' }\n      },"
    if marker not in content:
        raise SystemExit("deploy route marker not found")
    content = content.replace(marker, marker + releases_block)

path.write_text(content)
print("OK: frontend/src/router/index.ts fixed")

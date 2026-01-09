import os
import urllib.request

VENDOR_DIR = os.path.join("static", "vendor")

ASSET_SOURCES = [
    "https://unpkg.com",
    "https://cdn.jsdelivr.net/npm",
    "https://cdn.bootcdn.net/ajax/libs",
]

ASSET_TARGETS = [
    ("vue@3/dist/vue.global.prod.js", os.path.join(VENDOR_DIR, "vue.global.prod.js")),
    ("element-plus/dist/index.css", os.path.join(VENDOR_DIR, "element-plus", "index.css")),
    ("element-plus/dist/index.full.min.js", os.path.join(VENDOR_DIR, "element-plus", "index.full.min.js")),
    ("@element-plus/icons-vue/dist/index.iife.min.js", os.path.join(VENDOR_DIR, "icons", "index.iife.min.js")),
    ("swagger-ui-dist@5/swagger-ui.css", os.path.join(VENDOR_DIR, "swagger-ui-dist", "swagger-ui.css")),
    ("swagger-ui-dist@5/swagger-ui-bundle.js", os.path.join(VENDOR_DIR, "swagger-ui-dist", "swagger-ui-bundle.js")),
    ("swagger-ui-dist@5/swagger-ui-standalone-preset.js", os.path.join(VENDOR_DIR, "swagger-ui-dist", "swagger-ui-standalone-preset.js")),
]

BOOTCDN_MAP = {
    "vue@3/dist/vue.global.prod.js": "vue/3.5.12/vue.global.prod.js",
    "element-plus/dist/index.css": "element-plus/2.7.8/index.css",
    "element-plus/dist/index.full.min.js": "element-plus/2.7.8/index.full.min.js",
    "@element-plus/icons-vue/dist/index.iife.min.js": "element-plus-icons-vue/2.3.1/index.iife.min.js",
    "swagger-ui-dist@5/swagger-ui.css": "swagger-ui/5.17.14/swagger-ui.css",
    "swagger-ui-dist@5/swagger-ui-bundle.js": "swagger-ui/5.17.14/swagger-ui-bundle.js",
    "swagger-ui-dist@5/swagger-ui-standalone-preset.js": "swagger-ui/5.17.14/swagger-ui-standalone-preset.js",
}

ASSETS = []
for path, dst in ASSET_TARGETS:
    urls = []
    urls.append(f"https://unpkg.com/{path}")
    urls.append(f"https://cdn.jsdelivr.net/npm/{path}")
    bootcdn_path = BOOTCDN_MAP.get(path)
    if bootcdn_path:
        urls.append(f"https://cdn.bootcdn.net/ajax/libs/{bootcdn_path}")
    ASSETS.append((urls, dst))

def ensure_vendor_assets():
    os.makedirs(VENDOR_DIR, exist_ok=True)
    for _, dst in ASSETS:
        os.makedirs(os.path.dirname(dst), exist_ok=True)
    for urls, dst in ASSETS:
        try:
            if not os.path.exists(dst) or os.path.getsize(dst) == 0:
                for url in urls:
                    try:
                        req = urllib.request.Request(
                            url,
                            headers={
                                "User-Agent": "Mozilla/5.0",
                                "Accept": "*/*",
                            },
                        )
                        with urllib.request.urlopen(req, timeout=6) as resp:
                            data = resp.read()
                        if data:
                            with open(dst, "wb") as f:
                                f.write(data)
                            break
                    except Exception:
                        continue
        except Exception:
            pass

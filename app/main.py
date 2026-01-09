from fastapi import FastAPI
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
import os
import threading

from app.api.monitor_routes import router as monitor_router
from app.api.routes import router
from app.core.cdn_cache import ensure_vendor_assets
from app.inspection.routes import router as inspection_router
from app.monitor.engine import monitor_engine
from app.sync.task_manager import task_manager


app = FastAPI(
    title="Shark Platform",
    docs_url=None,
    redoc_url=None,
)

app.mount("/ui", StaticFiles(directory="static"), name="static")


@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url="/ui/index.html")


@app.get("/docs", include_in_schema=False)
async def docs():
    swagger_js_fs = os.path.join("static", "vendor", "swagger-ui-dist", "swagger-ui-bundle.js")
    swagger_css_fs = os.path.join("static", "vendor", "swagger-ui-dist", "swagger-ui.css")
    if os.path.exists(swagger_js_fs) and os.path.getsize(swagger_js_fs) > 0 and os.path.exists(swagger_css_fs) and os.path.getsize(swagger_css_fs) > 0:
        return get_swagger_ui_html(
            openapi_url=app.openapi_url,
            title=f"{app.title} - Docs",
            swagger_js_url="/ui/vendor/swagger-ui-dist/swagger-ui-bundle.js",
            swagger_css_url="/ui/vendor/swagger-ui-dist/swagger-ui.css",
            swagger_favicon_url="/ui/images/brand.png",
        )

    return HTMLResponse(
        content=(
            "<!doctype html><html><head><meta charset='utf-8' />"
            f"<title>{app.title} - Docs</title>"
            "<style>body{font-family:ui-sans-serif,system-ui,-apple-system;max-width:920px;margin:40px auto;padding:0 16px;line-height:1.6}"
            "code,pre{background:#f6f8fa;border:1px solid #e5e7eb;border-radius:8px;padding:2px 6px}"
            "pre{padding:12px;overflow:auto}a{color:#2563eb;text-decoration:none}a:hover{text-decoration:underline}</style>"
            "</head><body>"
            f"<h1>{app.title} 文档</h1>"
            "<p>当前环境未获取到 Swagger UI 静态资源，已提供可用的简化文档入口：</p>"
            "<ul>"
            "<li><a href='/openapi.json'>OpenAPI JSON</a></li>"
            "<li><a href='/ui/index.html'>Web 控制台</a></li>"
            "</ul>"
            "<p>如果需要 Swagger UI，请确保服务启动时可拉取静态资源，或将 swagger-ui-dist 资源放入：</p>"
            "<pre>static/vendor/swagger-ui-dist/</pre>"
            "</body></html>"
        ),
        status_code=200,
    )


app.include_router(router)
app.include_router(monitor_router, prefix="/monitor", tags=["monitor"])
app.include_router(inspection_router, prefix="/inspection", tags=["inspection"])


@app.on_event("startup")
def startup_restore_tasks():
    threading.Thread(target=ensure_vendor_assets, daemon=True).start()
    task_manager.restore_from_disk()
    monitor_engine.start()


@app.on_event("shutdown")
def shutdown_event():
    monitor_engine.stop()

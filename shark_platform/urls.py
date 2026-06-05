from django.contrib import admin
from django.urls import path, include, re_path
from django.views.static import serve
from django.conf import settings
from django.http import HttpResponse
import os

def serve_spa(request):
    # Only served in DEBUG mode or if Nginx is bypassed (which shouldn't happen in prod)
    try:
        # Check potential locations (Local vs Docker)
        possible_paths = [
            os.path.join(settings.BASE_DIR, 'frontend/dist/index.html'),
            os.path.join(settings.BASE_DIR, 'templates/index.html'), # Legacy/Docker backup
        ]
        for p in possible_paths:
            if os.path.exists(p):
                with open(p, 'r') as f:
                    return HttpResponse(f.read())
        
        return HttpResponse("Frontend index.html not found. Run 'npm run build' in frontend directory.", status=501)
    except Exception as e:
        return HttpResponse(f"Error serving SPA: {str(e)}", status=500)

def serve_static_from_root(request, path):
    """
    Serve static files from frontend/dist if they exist,
    otherwise fallback to index.html for SPA routing.
    """
    # Define root for frontend static files
    # Try frontend/dist first (new standard)
    doc_root = os.path.join(settings.BASE_DIR, 'frontend/dist')
    
    # Check if file exists
    fullpath = os.path.join(doc_root, path)
    if os.path.isfile(fullpath):
        return serve(request, path, document_root=doc_root)
        
    # Fallback to index.html for SPA routing
    return serve_spa(request)

urlpatterns = [
    path('admin/', admin.site.urls),
    
    # API Routes
    path('api/', include('api.urls')),
    path('api/deploy/', include('deploy.urls')),
    path('api/', include('tasks.urls')), 
    path('api/monitor/', include('monitor.urls')),
    path('api/inspection/', include('inspection.urls')),
    path('api/', include('schedules.urls')),
    path('api/ai_ops/', include('ai_ops.urls')),
    path('api/db/', include('db_manager.urls')),
    path('api/', include('traffic.urls')),
    path('api/releases/', include('releases.urls')),

    # Serve assets folder directly (Optimization)
    # Note: In production with Nginx, this is handled by Nginx.
    # This is kept for local development (DEBUG=True).
    re_path(r'^assets/(?P<path>.*)$', serve, {'document_root': os.path.join(settings.BASE_DIR, 'frontend/dist/assets')}),
    
    # Catch-all for everything else (root files like brand.png + SPA routes)
    # In production, Nginx handles this. Kept for dev.
    re_path(r'^(?P<path>.*)$', serve_static_from_root),
]

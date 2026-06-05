# 发版管理功能补丁包 (releases-feature)

基于 `etz/ops:v1.0.7` 增量添加 ECR Webhook → Argo CD 目标版本更新 + 发版管理页面。

## 文件清单

### 新增（直接复制）

```
releases/                          # Django 后端模块
frontend/src/api/releases.ts
frontend/src/views/Releases/Index.vue
```

### 需改动的现有文件（4 处）

| 文件 | 改动 |
|---|---|
| `shark_platform/settings.py` | `INSTALLED_APPS` 加 `'releases'`；logging 加 `releases`；底部加 4 个环境变量 |
| `shark_platform/urls.py` | 加 `path('api/releases/', include('releases.urls'))` |
| `frontend/src/router/index.ts` | 加 `/releases` 路由 |
| `frontend/src/components/Layout/AppSidebar.vue` | 加菜单项 + `Promotion` 图标 |

## 一键应用（在 v1.0.7 代码根目录）

```bash
cd /path/to/shark-Platform
bash patch/releases-feature/apply-patch.sh
```

## 环境变量（K8s Secret / docker-compose）

```bash
ARGOCD_URL=https://your-argocd.example.com
ARGOCD_USERNAME=admin
ARGOCD_PASSWORD=xxx
ECR_WEBHOOK_TOKEN=xxx
```

## 打镜像

本补丁脚本**只做代码改动**，不包含构建、登录 ECR、推送等操作。  
代码改完后，用你们自己的脚本打 **v1.0.8** 镜像并上传即可。

```bash
cd /path/to/ops-v1.0.7-code
bash patch/releases-feature/apply-patch.sh
# 然后执行你们现有的 build / push 脚本，tag 用 v1.0.8
```

## API 端点

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/releases/webhook/ecr` | EventBridge 回调（Header: `Authorization: Bearer $ECR_WEBHOOK_TOKEN`） |
| GET | `/api/releases/apps` | 应用列表 + 当前版本 |
| GET | `/api/releases/apps/{id}/history` | 历史版本 |
| POST | `/api/releases/apps/{id}/rollback` | 回滚 |
| GET/POST | `/api/releases/config` | 映射配置 CRUD |

## EventBridge Webhook 地址

```
POST https://ubest-ops.prd.exc888.org/api/releases/webhook/ecr
Authorization: Bearer <ECR_WEBHOOK_TOKEN>
```
